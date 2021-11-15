function Invoke-TervisShopifyInterfaceOrderImport {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )

    Write-Progress -Activity "Shopify Order Import Interface" -CurrentOperation "Setting environment variables"
    Set-TervisEBSEnvironment -Name $Environment 2> $null
    Set-TervisShopifyEnvironment -Environment $Environment
    
    $ShopName = Get-TervisShopifyEnvironmentShopName -Environment $Environment
    
    try {
        Write-Progress -Activity "Shopify Order Import Interface" -CurrentOperation "Getting orders from Shopify"
        [array]$ShopifyOrders = Get-TervisShopifyOrdersForImport -ShopName $ShopName
        if ($ShopifyOrders.Count -gt 0) {
            Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Information -EventId 1 `
                -Message "Starting Shopify order import. Processing $($ShopifyOrders.Count) order(s)." 
        }
    } catch {
        Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Error -EventId 2 `
        -Message "Something went wrong. Reason:`n$_`n$($_.InvocationInfo.PositionMessage)" 
            $_
    }
    $i = 0
    $OrdersProcessed = 0
    foreach ($Order in $ShopifyOrders) {
        $i++
        Write-Progress -Activity "Shopify Order Import Interface" -CurrentOperation "Importing orders to EBS" `
            -PercentComplete ($i * 100 / $ShopifyOrders.Count) -Status "$i of $($ShopifyOrders.Count)"
        try {
            if (
                -not $Order.StoreCustomerNumber -or
                -not $Order.Subinventory
            ) {throw "Location information incomplete. Please update LocationDefinition.csv."}
            if (-not (Test-TervisShopifyEBSOrderExists -Order $Order)) {
                $OrderObject = $Order | New-TervisShopifyOrderObject -ShopName $ShopName -ErrorAction Stop 
                $ParameterizedOrderObject = $OrderObject | ConvertTo-TervisShopifyEBSParameterizedValues
                $EBSQuery = $ParameterizedOrderObject.OrderObject | Convert-TervisShopifyOrderObjectToEBSQuery -ErrorAction Stop
                Invoke-EBSSQL -SQLCommand $EBSQuery -Parameters $ParameterizedOrderObject.Parameters -ErrorAction Stop
            }
            $IsBTO = $Order | Test-TervisShopifyBuildToOrder
            if ($IsBTO) {
                $OrderBTO = $Order | ConvertTo-TervisShopifyOrderBTO
                if (-not (Test-TervisShopifyEBSOrderExists -Order $OrderBTO)) {
                    try {
                        $OrderObjectBTO = $OrderBTO | New-TervisShopifyBuildToOrderObject -ErrorAction Stop 
                        $ParameterizedOrderObjectBTO = $OrderObjectBTO | ConvertTo-TervisShopifyEBSParameterizedValues -ErrorAction Stop
                        $EBSQueryBTO =  $ParameterizedOrderObjectBTO.OrderObject | Convert-TervisShopifyOrderObjectToEBSQuery -ErrorAction Stop
                        Invoke-EBSSQL -SQLCommand $EBSQueryBTO -Parameters $ParameterizedOrderObjectBTO.Parameters -ErrorAction Stop 
                    } catch {
                        $Order | Set-ShopifyOrderTag -ShopName $ShopName -AddTag "NeedsReview" | Out-Null
                    }
                }
            }
            $Order | Set-ShopifyOrderTag -ShopName $ShopName -AddTag "ImportedToEBS" | Out-Null
            $OrdersProcessed++
        } catch {
            Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Error -EventId 2 `
                -Message "Something went wrong importing Shopify order #$($Order.legacyResourceId). Reason:`n$_`n$($_.InvocationInfo.PositionMessage)" 
        }
    }

    try {
        Write-Progress -Activity "Shopify Order Import Interface" -CurrentOperation "Getting refunds from Shopify"
        [array]$ShopifyRefunds = Get-TervisShopifyOrdersWithRefundPending -ShopName $ShopName # | # Implement with payments  
            # Where-Object {$_.transactions.edges.node.gateway -notcontains "exchange-credit"}
        if ($ShopifyRefunds.Count -gt 0) {
            Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Information -EventId 1 `
                -Message "Starting Shopify refund import. Processing $($ShopifyRefunds.Count) refund(s)." 
        }
    } catch {
        Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Error -EventId 2 `
        -Message "Something went wrong. Reason:`n$_`n$($_.InvocationInfo.PositionMessage)" 
            $_
    }

    $i = 0
    $RefundsProcessed = 0
    foreach ($Refund in $ShopifyRefunds) {
        $i++
        Write-Progress -Activity "Shopify Order Import Interface" -CurrentOperation "Importing refunds to EBS" `
            -PercentComplete ($i * 100 / $ShopifyRefunds.Count) -Status "$i of $($ShopifyRefunds.Count)"
        try {
            if (
                -not $Refund.StoreCustomerNumber -or
                -not $Refund.Subinventory
            ) {throw "Location information incomplete. Please update LocationDefinition.csv."}
            if (
                -not (Test-TervisShopifyEBSOrderExists -Order $Refund) -and $Refund.refundLineItems.edges[0]
            ) {
                $RefundObject = $Refund | New-TervisShopifyOrderObject -ShopName $ShopName -ErrorAction Stop
                $ParameterizedRefundObject = $RefundObject | ConvertTo-TervisShopifyEBSParameterizedValues -ErrorAction Stop
                $EBSQuery = $ParameterizedRefundObject.OrderObject | Convert-TervisShopifyOrderObjectToEBSQuery -ErrorAction Stop
                Invoke-EBSSQL -SQLCommand $EBSQuery -Parameters $ParameterizedRefundObject.Parameters -ErrorAction Stop
            }
            $Refund.Order | Set-ShopifyOrderTag -ShopName $ShopName -RemoveTag $Refund.RefundTag -AddTag "RefundProcessed_$($Refund.RefundID)"
            $RefundsProcessed++
        } catch {
            Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Error -EventId 2 `
                -Message "Something went wrong importing refunds for order #$($Refund.Order.legacyResourceId). Reason:`n$_`n$($_.InvocationInfo.PositionMessage)" 
        }
    }
    Invoke-TervisShopifyRefundPendingTagCleanup -ShopName $ShopName
    if ($ShopifyOrders.Count -gt 0 -or $ShopifyRefunds.Count -gt 0) {
        Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Information -EventId 1 `
                -Message "Finished Shopify order import. Processed $OrdersProcessed order(s). Processed $RefundsProcessed refund(s)."
    }
}

function Test-TervisShopifyEBSOrderExists {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $Query = @"
            SELECT orig_sys_document_ref
            FROM xxoe_headers_iface_all
            WHERE orig_sys_document_ref = '$($Order.EBSDocumentReference)'
"@
        try {
            $Result = Invoke-EBSSQL -SQLCommand $Query
        } catch {
            throw "Could not connect to EBS to check order $($Order.EBSDocumentReference)"
        }
        if ($Result) {return $true} else {return $false}
    }
}

function Get-TervisShopifyPaymentTypeCode {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Transaction
    )
    process {
        switch ($Transaction.gateway) {
            "cash" { "CHECK"; break }
            "shopify_payments" { "CREDIT_CARD"; break }
            "Givex" { "CASH"; break } 
            default { "UNKNOWN" }
        }
    }
}

function Get-TervisShopifyPaymentCollectionEvent {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Transaction
    )
    process {
        switch ($Transaction.kind) {
            "sale" { "PREPAY"; break }
            "authorization" { "INVOICE"; break }
            default {""}
        }
    }
}

function New-TervisShopifyCCDummyNumber {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Transaction
    )
    process {
        if ($Transaction.payment_details.credit_card_number) {
            # return $Transaction.id.ToString().PadLeft(12,"0").Substring(0,12) + `
            #     $Transaction.payment_details.credit_card_number.Substring(15,4)
            return "4012888888881881"
        } else {
            return ""
        }
    }
}

function New-TervisShopifyCCName {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Transaction
    )
    process {
        $Name = $Transaction.receipt.source.name
        $Brand = $Transaction.receipt.source.brand
        $Last4 = $Transaction.receipt.source.last4
        return ("$Name $Brand $Last4").Trim()
    }
}

function Get-TervisShopifyReceiptMethod {
    param (
        # [Parameter(Mandatory)]$ReceiptMethodId,
        [Parameter(Mandatory)]$PaymentTypeCode
    )
    if (-not $Script:CCReceiptMethodId) {
        $Query = "SELECT receipt_method_id FROM apps.AR_RECEIPT_METHODS WHERE name = 'SHOPIFY-Credit Card'"
        $Script:CCReceiptMethodId = Invoke-EBSSQL -SQLCommand $Query | Select-Object -ExpandProperty RECEIPT_METHOD_ID
        if (-not $Script:CCReceiptMethodId) { throw "Could not retrieve receipt method from EBS." }
    }
    switch ($PaymentTypeCode) {
        # "CHECK" { $ReceiptMethodId; break } # This is now handled in EBS
        "CASH" { "8001"; break }
        "CREDIT_CARD" { "$Script:CCReceiptMethodId"; break }
        default {""}

    }
}

function New-TervisShopifyOrderObject {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order,
        [Parameter(Mandatory)]$ShopName
    )
    process {
        # Initial order object
        $OrderObject = $Order | New-TervisShopifyOrderObjectBase

        # Order lines conversion, for both sales and refunds
        $OrderObject.LineItems += $Order | New-TervisShopifyOrderObjectLines

        # Order payments conversion
        $OrderObject.Payments += $Order | New-TervisShopifyOrderObjectPayments -ShopName $ShopName

        # Add refund information to OrderObject headers
        $OrderObject | Add-TervisShopifyRefundOrderHeaderFields -Order $Order

        return $OrderObject
    }
}

function ConvertTo-TervisShopifyEBSParameterizedValues {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$OrderObject
    )
    begin {
        $ModuleBase = (Get-Module -Name TervisShopifyPowerShellApplication).ModuleBase
        $HumanInputFields = (Import-PowerShellDataFile -Path $ModuleBase\Config\HumanInputFields.psd1).HumanInputFields
    }
    process {
        $ParameterCount = 0
        $Parameters = @()
        $Tables = $OrderObject | 
            Get-Member | 
            Where-Object MemberType -eq NoteProperty | 
            Select-Object -ExpandProperty Name
        foreach ($Table in $Tables) {
            $OrderObject."$Table" | ForEach-Object {
                $TableObject = $_
                $Keys = $TableObject | 
                    Get-Member | 
                    Where-Object Name -in $HumanInputFields |
                    Select-Object -ExpandProperty Name

                foreach ($Key in $Keys) {
                    $ParameterCount++
                    $ParameterName = "p$ParameterCount"
                    $Value = $TableObject."$Key"
        
                    switch ($Value)
                    {   
                        { $_ -eq "sysdate" } { Write-Host -ForegroundColor Cyan "[VAR] $Table.$Key = $_"; break }
                        { $_ -match "TO_DATE" } {
                            $DateCode = $_.Substring(9,14)
                            $Parameters += [PSCustomObject]@{
                                ParameterName = $ParameterName
                                DbType = "Varchar2"
                                Obj = $DateCode
                                Direction = "Input"
                            }
                            $TableObject."$Key" = "TO_DATE(:$ParameterName, 'YYYYMMDDHH24MISS')"
                            # Write-Host -ForegroundColor Magenta "[CMD] $Table.$Key = $($TableObject."$Key") - $($Parameters[-1].obj)"                            
                            break
                        }
                        { $_[0] -eq "'" -or $_.ToString()[0] -eq "-" } {
                            $TableObject."$Key" = ":$ParameterName"
                            $Parameters += [PSCustomObject]@{
                                ParameterName = $ParameterName
                                DbType = "Varchar2"
                                Obj = $_.ToString().Trim("'")
                                Direction = "Input"
                            }
                            # Write-Host -ForegroundColor Green "[STR] $Table.$Key = $($TableObject."$Key") - $($Parameters[-1].obj)"
                            break
                        }
                        { $_[0] -match "\d" } {
                            $Parameters += [PSCustomObject]@{
                                ParameterName = $ParameterName
                                DbType = "Double"
                                Obj = $_
                                Direction = "Input"
                            }
                            $TableObject."$Key" = ":$ParameterName"
                            # Write-Host -ForegroundColor Blue "[NUM] $Table.$Key = $($TableObject."$Key") - $($Parameters[-1].obj)"
                            break
                        }
                        default { 
                            # Write-Host -ForegroundColor Red "[NUL] $Table.$Key = $_" 
                        }
                    }
                }
            }
        }
        return [PSCustomObject]@{
            OrderObject = $OrderObject
            Parameters = $Parameters
        }
    }
}

function Test-TervisShopifyBuildToOrder {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        if ($Order.CustomAttributes.shipMethod -ne $null) {
            return $true
        } else {
            return $false
        }
    }
}

function Invoke-TervisShopifyLineItemSkuSubstitution {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$LineItem
    )
    process {
        $NewSKU = $null
        $CustomAttributes = $LineItem | Convert-TervisShopifyCustomAttributesToObject
        if ($LineItem.sku) {
            switch ($LineItem.sku) {
                # Substitutes custom shipping SKU in Shopify with the "FREIGHT" EBS item number
                "Shipping-Standard"     { $NewSKU = "1097271"; break }
                "Shipping-Overnight"    { $NewSKU = "1097271"; break }
                "Shipping-Extended"     { $NewSKU = "1097271"; break }
                "Shipping-MFL"          { $NewSKU = "1097271"; break }
                # Substitutes GivexAct Shopify SKU with "GIFT CARD" EBS item number
                "GivexAct10"    { $NewSKU = "1314274"; break } 
                "GivexAct25"    { $NewSKU = "1314274"; break } 
                "GivexAct50"    { $NewSKU = "1314274"; break } 
                "GivexAct100"   { $NewSKU = "1314274"; break } 
                # Else just keep existing SKU
                Default { $NewSku = $LineItem.sku}
            }
        } elseif ($CustomAttributes.missingItem) {
            $NewSku = $CustomAttributes.missingItem
        }
        $LineItem | Add-Member -MemberType NoteProperty -Name sku -Value $NewSKU -Force
    }
}

function New-TervisShopifyOrderObjectBase {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $OrderedDate = $Order.createdAt | ConvertTo-TervisShopifyOracleSqlDateString
    
        [PSCustomObject]@{
            Header = [PSCustomObject]@{
                ORDER_SOURCE_ID = "'1022'" # For use during testing payments
                ORIG_SYS_DOCUMENT_REF = "'$($Order.EBSDocumentReference)'"
                ORDERED_DATE = $OrderedDate
                ORDER_TYPE = "'Store Order'"
                CUSTOMER_NUMBER = "'$($Order.StoreCustomerNumber)'"
                BOOKED_FLAG = "'Y'"
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                PROCESS_FLAG = "'N'"
                SOURCE_NAME = "'RMS'"
                OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
                CREATED_BY_NAME = "'SHOPIFY'"
                LAST_UPDATED_BY_NAME = "'SHOPIFY'"
                # Free freight may be needed on original order 

                # Refund fields
                GLOBAL_ATTRIBUTE9 = "''" # Cash
                GLOBAL_ATTRIBUTE10 = "''" # Credit card
                GLOBAL_ATTRIBUTE11 = "''" # Gift card
            }
            Customer = [PSCustomObject]@{}
            LineItems = @()
            Payments = @()
        }
    }
}

function New-TervisShopifyOrderObjectLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $LineItems = @()
        $LineItemCounter = 0
        $IsRefund = $Order.id -match "refund"
        $LineItemType = if ($IsRefund) {"refundLineItems"} else {"lineItems"}

        if (-not $IsRefund) { $Order | Add-TervisShopifyShippingLineItem }
        # Revisit this when adding refund value to EBS
        # else {
        #     $Order | Set-TervisShopifyRefundLineItemPricesToZero
        #     $Order | Add-TervisShopifyTotalRefundSetLineItem
        # }
        $Order.$LineItemType.edges.node | Invoke-TervisShopifyLineDiscountRecalculation -IsRefund:$IsRefund
        $Order | Split-TervisShopifyOrderSpecialOrderLines
        
        $LineItems += foreach ($Line in $Order.$LineItemType.edges.node) {
            if (
                $Line.quantity -ne 0 -and
                $Line.name -notlike "Personalization for*" -and
                $Line.lineItem.name -notlike "Personalization for*"
            ) {

                $LineItemCounter++
    
                $Line | Invoke-TervisShopifyLineItemSkuSubstitution
                $LineType = $Line | Get-TervisShopifyLineItemType -IsRefund:$IsRefund

                if ($IsRefund) {
                    $InventoryItem = $Line.lineItem.sku.ToUpper()
                    $OrderedQuantity = $Line.quantity * -1
                    $UnitListPrice = $Line.lineItem.originalUnitPriceSet.shopMoney.amount
                    $UnitSellingPrice = $Line.lineItem.discountedUnitPriceSet.shopMoney.amount
                    $ReturnReasonCode = "STORE RETURN"
                    $TaxValue = $Line.totalTaxSet.shopMoney.amount
                    # $TaxValue = 0 # For refunds only, since tax is included in totalRefundSet
                } else {
                    $InventoryItem = $Line.sku.ToUpper()
                    $OrderedQuantity = $Line.quantity
                    $UnitListPrice = $Line.originalUnitPriceSet.shopMoney.amount
                    $UnitSellingPrice = $Line.discountedUnitPriceSet.shopMoney.amount
                    $TaxValue = $Line.taxLines.priceSet.shopMoney.amount | Measure-Object -Sum | Select-Object -ExpandProperty Sum
                }
    
                [PSCustomObject]@{
                    ORDER_SOURCE_ID = "'1022'" # For use during testing payments
                    ORIG_SYS_DOCUMENT_REF = "'$($Order.EBSDocumentReference)'"
                    ORIG_SYS_LINE_REF = "'$LineItemCounter'"
                    ORIG_SYS_SHIPMENT_REF = "''"
                    LINE_TYPE = "'$LineType'"
                    INVENTORY_ITEM = "'$InventoryItem'"
                    ORDERED_QUANTITY = $OrderedQuantity
                    ORDER_QUANTITY_UOM = "'EA'"
                    SHIP_FROM_ORG = "'STO'"
                    UNIT_LIST_PRICE = $UnitListPrice
                    UNIT_SELLING_PRICE = $UnitSellingPrice
                    CALCULATE_PRICE_FLAG = "'P'"
                    RETURN_REASON_CODE = "'$ReturnReasonCode'"
                    CREATION_DATE = $Order.createdAt | ConvertTo-TervisShopifyOracleSqlDateString
                    LAST_UPDATE_DATE = $Order.createdAt | ConvertTo-TervisShopifyOracleSqlDateString
                    SUBINVENTORY = "'$($Order.Subinventory)'"
                    PROCESS_FLAG = "'N'"
                    SOURCE_NAME = "'RMS'"
                    OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
                    CREATED_BY_NAME = "'SHOPIFY'"
                    LAST_UPDATED_BY_NAME = "'SHOPIFY'"
                    TAX_VALUE = $TaxValue
                    # TAX_VALUE = "''" # For use in PRD until payments implemented
                }
            }
        }

        return $LineItems
    }
}

function Invoke-TervisShopifyLineDiscountRecalculation {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Line,
        [switch]$IsRefund
    )
    process {
        if ($IsRefund) {
            $LineItem = $Line.lineItem
        } else {
            $LineItem = $Line
        }
        $ListUnitPrice = $LineItem.originalUnitPriceSet.shopMoney.amount
        $DiscountedUnitPrice = $LineItem.discountedUnitPriceSet.shopMoney.amount
        $DiscountedLineTotal = $LineItem.discountedTotalSet.shopMoney.amount
        
        if ($ListUnitPrice -eq $DiscountedUnitPrice -or $LineItem.quantity -eq 0) { return }
        $ActualDiscountedPricePerUnit = $DiscountedLineTotal / $LineItem.quantity
        $LineItem.discountedUnitPriceSet.shopMoney.amount = $ActualDiscountedPricePerUnit
    }
}

function Split-TervisShopifyOrderSpecialOrderLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $LineItems = $Order.lineItems.edges.node
        foreach ($Line in $LineItems) {
            $CustomAttributes = $Line | Convert-TervisShopifyCustomAttributesToObject
            
            if ($CustomAttributes.isSpecialOrder -ne "true") { continue }
            
            # If entire line is a special order, don't split out the lines
            if (
                -not $CustomAttributes.specialOrderQuantity -or
                $Line.quantity -eq $CustomAttributes.specialOrderQuantity
            ) {
                $Line.CustomAttributes = $Line.CustomAttributes | 
                    Where-Object {$_.key -ne "specialOrderQuantity"}
                continue
            }

            # Add new special order lines to order
            $LineCopy = $Line | ConvertTo-Json -Depth 10 -Compress | ConvertFrom-Json
            $LineCopy.quantity = [int]$CustomAttributes.specialOrderQuantity
            $LineCopy.taxLines = [PSCustomObject]@{
                priceSet = [PSCustomObject]@{
                    shopMoney = [PSCustomObject]@{
                        amount = 0
                    }
                }
            }
            $Order.lineItems.edges += [PSCustomObject]@{node = $LineCopy}

            # Cleanse original line of special order properties
            $Line.quantity = $Line.quantity - $CustomAttributes.specialOrderQuantity
            $Line.CustomAttributes = $Line.CustomAttributes | Where-Object {$_.key -notlike "*specialOrder*"}
        }
    }    
}

function Add-TervisShopifyShippingLineItem {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        if (-not $Order.IsOnlineOrder -or -not $Order.shippingLine) { return }
        $Shipping = $Order.shippingLine
        $ShippingNode = [PSCustomObject]@{
            node = [PSCustomObject]@{
                name = "FREIGHT"
                sku = "1097271"
                quantity = 1
                originalUnitPriceSet = $Shipping.discountedPriceSet
                discountedUnitPriceSet = $Shipping.discountedPriceSet
                taxLines = $Shipping.taxLines
            }
        }
        $Order.lineItems.edges += $ShippingNode
    }
}

function Get-TervisShopifyLineItemType {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Line,
        [switch]$IsRefund
    )
    process {
        if ($IsRefund) {
            return "Tervis Credit Only Line"
        }
        
        $CustomAttributes = $Line | Convert-TervisShopifyCustomAttributesToObject
        if (
            $Line.sku -like "*P" -or
            $CustomAttributes.isSpecialOrder -eq "true" -or
            $Line.name -like "STORES CUSTOM*"
        ) {
            return "Tervis Bill Only Line"
        } else {
            return "Tervis Bill Only with Inv Line"
        }
    }
}

function Set-TervisShopifyRefundLineItemPricesToZero {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Refund
    )
    process {
        foreach ($Node in $Refund.refundLineItems.edges.node) {
            $Node.priceSet.shopMoney.amount = 0
        }
    }
}

function Add-TervisShopifyTotalRefundSetLineItem {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Refund
    )
    process {
        $TotalRefundSetNode = [PSCustomObject]@{
            node = [PSCustomObject]@{
                lineItem = [PSCustomObject]@{
                    sku = "1097269" # Random miscellaneous item sku
                }
                priceSet = $Refund.totalRefundedSet
                quantity = 1
                # totalTaxSet here may be okay as zero. Need to revisit when implementing payments.
                totalTaxSet = [PSCustomObject]@{
                    shopMoney = [PSCustomObject]@{
                        amount = 0 
                    }
                }
            }
        }
        $Refund.refundLineItems.edges += $TotalRefundSetNode
    }
}

function New-TervisShopifyOrderObjectPayments {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order,
        [Parameter(Mandatory)]$ShopName
    )
    process {
        if (
            -not $Order.LegacyResourceId -or # while returns don't have payment information only 
            $Order.IsOnlineOrder # until we are ready to process payments from QuickShip. Need to figure out blank exp date on Apple Pay
        ) { return }
        [array]$Transactions = $Order | Get-ShopifyRestOrderTransactionDetail -ShopName $ShopName
        
        foreach ($Transaction in $Transactions) {
            $ORIG_SYS_DOCUMENT_REF = $Order.EBSDocumentReference
            $PaymentTypeCode = $Transaction | Get-TervisShopifyPaymentTypeCode
            $PaymentCollectionEvent = $Transaction | Get-TervisShopifyPaymentCollectionEvent
            $CreditCardNumber = $Transaction | New-TervisShopifyCCDummyNumber
            $CreditCardName = $Transaction | New-TervisShopifyCCName
            $CreditCardCode = if ($Transaction.gateway -eq "shopify_payments") {"UNKNOWN"}
            $CreditCardApprovalDate = if ($CreditCardNumber) {
                    $Transaction.processed_at | ConvertTo-TervisShopifyOracleSqlDateString
                } else {
                    "''"
                }
            # $CheckNumber = if ($PaymentTypeCode -eq "CHECK") {""}
            # $ReceiptMethodId = Get-TervisShopifyReceiptMethod -ReceiptMethodId $Order.ReceiptMethodId -PaymentTypeCode $PaymentTypeCode # if ($PaymentTypeCode -eq "CHECK") {$Order.ReceiptMethodId}
            $CreationDate = $Transaction.processed_at | ConvertTo-TervisShopifyOracleSqlDateString
            $CreditCardExpiration = $Transaction | Get-TervisShopifyCCExpirationDate
            $ReceiptMethodId = Get-TervisShopifyReceiptMethod -PaymentTypeCode $PaymentTypeCode

            [PSCustomObject]@{
                # ORDER_SOURCE_ID = "1101"
                ORDER_SOURCE_ID = 1022 # For use during testing payments
                ORIG_SYS_DOCUMENT_REF = "'$ORIG_SYS_DOCUMENT_REF'"
                ORIG_SYS_PAYMENT_REF = "'$($Transaction.id)'"
                PAYMENT_TYPE_CODE = "'$PaymentTypeCode'"
                PAYMENT_COLLECTION_EVENT = "'$PaymentCollectionEvent'"
                CREDIT_CARD_NUMBER = "'$CreditCardNumber'"
                CREDIT_CARD_HOLDER_NAME = "'$CreditCardName'"
                CREDIT_CARD_CODE = "'$CreditCardCode'"
                # CREDIT_CARD_CODE = ''
                CREDIT_CARD_APPROVAL_CODE = "'$($Transaction.authorization)'"
                CREDIT_CARD_APPROVAL_DATE = "$CreditCardApprovalDate"
                # CHECK_NUMBER = $CheckNumber
                PAYMENT_AMOUNT = $Transaction.amount
                CREATION_DATE = "$CreationDate"
                LAST_UPDATE_DATE = "sysdate"
                CREDIT_CARD_EXPIRATION_MONTH = $CreditCardExpiration.Month
                CREDIT_CARD_EXPIRATION_YEAR = $CreditCardExpiration.Year
                CREDIT_CARD_PAYMENT_STATUS = "''"
                PROCESS_FLAG = "'N'"
                # SOURCE_NAME = 'SPF-OSP'
                SOURCE_NAME = "'RMS'" # For use during testing payments
                OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
                CREATED_BY_NAME = "'SHOPIFY'"
                LAST_UPDATED_BY_NAME = "'SHOPIFY'"
                RECEIPT_METHOD_ID = "'$($ReceiptMethodId)'"
                PAYMENT_TRX_ID = "'$($Transaction.id)'"
            }
        }
    }
}

function Get-TervisShopifyCCExpirationDate {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Transaction
    )
    process {
        # # Shopify keeps changing the API here, so spoofing it
        # if ($Transaction.receipt.payment_method_details.card.exp_month) {
        #     $Month = $Transaction.receipt.payment_method_details.card.exp_month
        #     $Year = $Transaction.receipt.payment_method_details.card.exp_year
        # } elseif ($Transaction.receipt.charges.data) {
        #     $Month = $Transaction.receipt.charges.data[0].payment_method_details.card_present.exp_month
        #     $Year = $Transaction.receipt.charges.data[0].payment_method_details.card_present.exp_year
        # }

        $ExpDate = (Get-Date).AddYears(1)
        $Month = $ExpDate.Month
        $Year = $ExpDate.Year
        
        return [PSCustomObject]@{
            Month = "'$Month'"
            Year = "'$Year'"
        }
    }
}

function Add-TervisShopifyRefundOrderHeaderFields {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$OrderObject,
        $Order
    )
    process {
        if (-not $Order.refundLineItems) { return }
        $RefundAmounts = $Order | ConvertTo-TervisShopifyDiscreteRefunds
        $OrderObject.Header.GLOBAL_ATTRIBUTE9 = $RefundAmounts.Cash 
        $OrderObject.Header.GLOBAL_ATTRIBUTE10 = $RefundAmounts.CreditCard
        $OrderObject.Header.GLOBAL_ATTRIBUTE11 = $RefundAmounts.GiftCard
    }
}

function ConvertTo-TervisShopifyDiscreteRefunds {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        if (-not $Order.refundLineItems) { return }
        $RefundAmounts = [PSCustomObject]@{
            Cash = "0"
            CreditCard = "0"
            GiftCard = "0"
        }
        foreach ($Node in $Order.transactions.edges.node) {
            switch ($Node.gateway) {
                "cash" {
                    $RefundAmounts.Cash =  $Node.amountSet.shopMoney.amount
                }
                "shopify_payments" {
                    $RefundAmounts.CreditCard =  $Node.amountSet.shopMoney.amount
                }
                "Givex" {
                    $RefundAmounts.GiftCard = $Node.amountSet.shopMoney.amount
                }
            }
        }
        return $RefundAmounts
    }
}
