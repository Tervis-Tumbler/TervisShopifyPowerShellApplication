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
        Write-Progress -Activity "Shopify Order Import Interface" -CurrentOperation "Getting refunds from Shopify"
        [array]$ShopifyRefunds = Get-TervisShopifyOrdersWithRefundPending -ShopName $ShopName # | # Implement with payments  
            # Where-Object {$_.transactions.edges.node.gateway -notcontains "exchange-credit"}
        if ($ShopifyOrders.Count -gt 0 -or $ShopifyRefunds.Count -gt 0) {
            Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Information -EventId 1 `
                -Message "Starting Shopify order import. Processing $($ShopifyOrders.Count) order(s), $($ShopifyRefunds.Count) refund(s)." 
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
            -PercentComplete ($i * 100 / $ShopifyOrders.Count)
        try {
            if (
                -not $Order.StoreCustomerNumber -or
                -not $Order.Subinventory -or
                -not $Order.ReceiptMethodId
            ) {throw "Location information incomplete. Please update LocationDefinition.csv."}
            if (-not (Test-TervisShopifyEBSOrderExists -Order $Order)) {
                <#
                Original order import process:

                $ConvertedOrderHeader = $Order | Convert-TervisShopifyOrderToEBSOrderLineHeader
                $ConvertedOrderLines = $Order | Convert-TervisShopifyOrderToEBSOrderLines
                $ConvertedOrderPayment = $Order | Convert-TervisShopifyPaymentsToEBSPayment -ShopName $ShopName # Need to account for split payments
                [array]$Subqueries = $ConvertedOrderHeader | New-EBSOrderLineHeaderSubquery
                $Subqueries += $ConvertedOrderLines | New-EBSOrderLineSubquery
                # $Subqueries += $ConvertedOrderPayment | New-EBSOrderLinePaymentSubquery # Comment in PRD until payments impleemented
                $Subqueries | Invoke-EBSSubqueryInsert
                #>

                # New order import process
                $EBSQuery = $Order | New-TervisShopifyOrderObject | Convert-TervisShopifyOrderObjectToEBSQuery 
                Invoke-EBSSQL -SQLCommand $EBSQuery
            }
            $IsBTO = $Order | Test-TervisShopifyBuildToOrder
            if ($IsBTO) {
                $OrderBTO = $Order | ConvertTo-TervisShopifyOrderBTO
                $EBSQueryBTO = $OrderBTO | New-TervisShopifyBuildToOrderObject | Convert-TervisShopifyOrderObjectToEBSQuery
                Invoke-EBSSQL -SQLCommand $EBSQueryBTO
            }
            $Order | Set-ShopifyOrderTag -ShopName $ShopName -AddTag "ImportedToEBS" | Out-Null
            $OrdersProcessed++
        } catch {
            Write-EventLog -LogName Shopify -Source "Order Interface" -EntryType Error -EventId 2 `
                -Message "Something went wrong importing Shopify order #$($Order.legacyResourceId). Reason:`n$_`n$($_.InvocationInfo.PositionMessage)" 
        }
    }
    $i = 0
    $RefundsProcessed = 0
    foreach ($Refund in $ShopifyRefunds) {
        $i++
        Write-Progress -Activity "Shopify Order Import Interface" -CurrentOperation "Importing refunds to EBS" `
            -PercentComplete ($i * 100 / $ShopifyRefunds.Count)
        try {
            if (
                -not $Refund.StoreCustomerNumber -or
                -not $Refund.Subinventory
            ) {throw "Location information incomplete. Please update LocationDefinition.csv."}
            if (-not (Test-TervisShopifyEBSOrderExists -Order $Refund)) {
                $ConvertedRefundHeader = $Refund | Convert-TervisShopifyOrderToEBSOrderLineHeader
                $ConvertedRefundLines = $Refund | Convert-TervisShopifyRefundToEBSOrderLines
                [array]$Subqueries = $ConvertedRefundHeader | New-EBSOrderLineHeaderSubquery
                $Subqueries += $ConvertedRefundLines | New-EBSOrderLineSubquery
                $Subqueries | Invoke-EBSSubqueryInsert
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

function Convert-TervisShopifyOrderToEBSOrderLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $OrderLineNumber = 0
        $Order.lineItems.edges.node | ForEach-Object {
            $OrderLineNumber++
            [PSCustomObject]@{
                ORDER_SOURCE_ID = "1022" # For use during testing payments
                ORIG_SYS_DOCUMENT_REF = $Order.EBSDocumentReference
                ORIG_SYS_LINE_REF = "$OrderLineNumber"
                ORIG_SYS_SHIPMENT_REF = ""
                LINE_TYPE = "Tervis Bill Only with Inv Line"
                INVENTORY_ITEM = "$($_.sku)"
                ORDERED_QUANTITY = $_.quantity
                ORDER_QUANTITY_UOM = "EA"
                SHIP_FROM_ORG = "STO"
                PRICE_LIST = ""
                UNIT_LIST_PRICE = $($_.originalUnitPriceSet.shopMoney.amount)
                UNIT_SELLING_PRICE = $($_.discountedUnitPriceSet.shopMoney.amount)
                CALCULATE_PRICE_FLAG = "P"
                RETURN_REASON_CODE = ""
                CUSTOMER_ITEM_ID_TYPE = ""
                ATTRIBUTE1 = ""
                ATTRIBUTE7 = ""
                ATTRIBUTE13 = ""
                ATTRIBUTE14 = ""
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                SUBINVENTORY = $Order.Subinventory
                EARLIEST_ACCEPTABLE_DATE = ""
                LATEST_ACCEPTABLE_DATE = ""
                GIFT_MESSAGE = ""
                SIDE1_COLOR = ""
                SIDE2_COLOR = ""
                SIDE1_FONT = ""
                SIDE2_FONT = ""
                SIDE1_TEXT1 = ""
                SIDE2_TEXT1 = ""
                SIDE1_TEXT2 = ""
                SIDE2_TEXT2 = ""
                SIDE1_TEXT3 = ""
                SIDE2_TEXT3 = ""
                SIDE1_INITIALS = ""
                SIDE2_INITIALS = ""
                PROCESS_FLAG = "N"
                SOURCE_NAME = "RMS"
                OPERATING_UNIT_NAME = "Tervis Operating Unit"
                CREATED_BY_NAME = "SHOPIFY"
                LAST_UPDATED_BY_NAME = "SHOPIFY"
                ACCESSORY = ""
                # TAX_VALUE = $_.taxLines.priceSet.shopMoney.amount | Measure-Object -Sum | Select-Object -ExpandProperty Sum
                TAX_VALUE = "" # For use in PRD until payments implemented
            }
        }
    }
}

function Convert-TervisShopifyRefundToEBSOrderLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $OrderLineNumber = 0
        $Order.refundLineItems.edges.node | ForEach-Object {
            $OrderLineNumber++
            [PSCustomObject]@{
                ORDER_SOURCE_ID = "1022" # For use during testing payments
                ORIG_SYS_DOCUMENT_REF = $Order.EBSDocumentReference
                ORIG_SYS_LINE_REF = "$OrderLineNumber"
                ORIG_SYS_SHIPMENT_REF = ""
                LINE_TYPE = "Tervis Credit Only Line"
                INVENTORY_ITEM = "$($_.lineItem.sku)"
                ORDERED_QUANTITY = $_.quantity * -1
                ORDER_QUANTITY_UOM = "EA"
                SHIP_FROM_ORG = "STO"
                PRICE_LIST = ""
                UNIT_LIST_PRICE = $($_.priceSet.shopMoney.amount)
                UNIT_SELLING_PRICE = $($_.priceSet.shopMoney.amount)
                CALCULATE_PRICE_FLAG = "P"
                RETURN_REASON_CODE = "STORE RETURN"
                CUSTOMER_ITEM_ID_TYPE = ""
                ATTRIBUTE1 = ""
                ATTRIBUTE7 = ""
                ATTRIBUTE13 = ""
                ATTRIBUTE14 = ""
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                SUBINVENTORY = $Order.Subinventory
                EARLIEST_ACCEPTABLE_DATE = ""
                LATEST_ACCEPTABLE_DATE = ""
                GIFT_MESSAGE = ""
                SIDE1_COLOR = ""
                SIDE2_COLOR = ""
                SIDE1_FONT = ""
                SIDE2_FONT = ""
                SIDE1_TEXT1 = ""
                SIDE2_TEXT1 = ""
                SIDE1_TEXT2 = ""
                SIDE2_TEXT2 = ""
                SIDE1_TEXT3 = ""
                SIDE2_TEXT3 = ""
                SIDE1_INITIALS = ""
                SIDE2_INITIALS = ""
                PROCESS_FLAG = "N"
                SOURCE_NAME = "RMS"
                OPERATING_UNIT_NAME = "Tervis Operating Unit"
                CREATED_BY_NAME = "SHOPIFY"
                LAST_UPDATED_BY_NAME = "SHOPIFY"
                ACCESSORY = ""
                # TAX_VALUE = $_.totalTaxSet.shopMoney.amount
                TAX_VALUE = "" # For use in PRD until payments implemented
            }
        }
    }
}

function Convert-TervisShopifyOrderToEBSOrderLines_Unified {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $IsRefund = $Order.id -match "refund"
        $LineItemType = if ($IsRefund) {"refundLineItems"} else {"lineItems"}
        $OrderLineNumber = 0
        $Order.$LineItemType.edges.node | ForEach-Object {
            $OrderLineNumber++

            if ($IsRefund) {
                $LineType = "Tervis Credit Only Line"
                $InventoryItem = $_.lineItem.sku
                $OrderedQuantity = $_.quantity * -1
                $UnitListPrice = $_.priceSet.shopMoney.amount
                $UnitSellingPrice = $_.priceSet.shopMoney.amount
                $ReturnReasonCode = "STORE RETURN"
                $TaxValue = $_.totalTaxSet.shopMoney.amount
            } else {
                $LineType = "Tervis Bill Only with Inv Line"
                $InventoryItem = $_.sku
                $OrderedQuantity = $_.quantity
                $UnitListPrice = $_.originalUnitPriceSet.shopMoney.amount
                $UnitSellingPrice = $_.discountedUnitPriceSet.shopMoney.amount
                $TaxValue = $_.taxLines.priceSet.shopMoney.amount | Measure-Object -Sum | Select-Object -ExpandProperty Sum
            }

            [PSCustomObject]@{
                ORDER_SOURCE_ID = "1022" # For use during testing payments
                ORIG_SYS_DOCUMENT_REF = $Order.EBSDocumentReference
                ORIG_SYS_LINE_REF = $OrderLineNumber
                ORIG_SYS_SHIPMENT_REF = ""
                LINE_TYPE = $LineType
                INVENTORY_ITEM = $InventoryItem
                ORDERED_QUANTITY = $OrderedQuantity
                ORDER_QUANTITY_UOM = "EA"
                SHIP_FROM_ORG = "STO"
                PRICE_LIST = ""
                UNIT_LIST_PRICE = $UnitListPrice
                UNIT_SELLING_PRICE = $UnitSellingPrice
                CALCULATE_PRICE_FLAG = "P"
                RETURN_REASON_CODE = $ReturnReasonCode
                CUSTOMER_ITEM_ID_TYPE = ""
                ATTRIBUTE1 = ""
                ATTRIBUTE7 = ""
                ATTRIBUTE13 = ""
                ATTRIBUTE14 = ""
                CREATION_DATE = $Order.createdAt | ConvertTo-TervisShopifyOracleSqlDateString
                LAST_UPDATE_DATE = $Order.createdAt | ConvertTo-TervisShopifyOracleSqlDateString
                SUBINVENTORY = $Order.Subinventory
                EARLIEST_ACCEPTABLE_DATE = ""
                LATEST_ACCEPTABLE_DATE = ""
                GIFT_MESSAGE = ""
                SIDE1_COLOR = ""
                SIDE2_COLOR = ""
                SIDE1_FONT = ""
                SIDE2_FONT = ""
                SIDE1_TEXT1 = ""
                SIDE2_TEXT1 = ""
                SIDE1_TEXT2 = ""
                SIDE2_TEXT2 = ""
                SIDE1_TEXT3 = ""
                SIDE2_TEXT3 = ""
                SIDE1_INITIALS = ""
                SIDE2_INITIALS = ""
                PROCESS_FLAG = "N"
                SOURCE_NAME = "RMS"
                OPERATING_UNIT_NAME = "Tervis Operating Unit"
                CREATED_BY_NAME = "SHOPIFY"
                LAST_UPDATED_BY_NAME = "SHOPIFY"
                ACCESSORY = ""
                # TAX_VALUE = $TaxValue
                TAX_VALUE = "" # For use in PRD until payments implemented
            }
        }
    }
}

function Convert-TervisShopifyOrderToEBSOrderLineHeader {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $IsPersonalized = $Order | Test-TervisShopifyIsPersonalizedOrder
        
        [PSCustomObject]@{
            ORDER_SOURCE_ID = "1022" # For use during testing payments
            ORIG_SYS_DOCUMENT_REF = $Order.EBSDocumentReference
            ORDERED_DATE = $Order.createdAt | ConvertTo-TervisShopifyOracleSqlDateString
            ORDER_TYPE = "Store Order"
            PRICE_LIST = ""
            SALESREP = ""
            PAYMENT_TERM = ""
            SHIPMENT_PRIORITY_CODE = ""
            SHIPPING_METHOD_CODE = ""
            SHIPMENT_PRIORITY = ""
            SHIPPING_INSTRUCTIONS = ""
            CUSTOMER_PO_NUMBER = ""
            SHIP_FROM_ORG = "ORG"
            SHIP_TO_ORG = ""
            INVOICE_TO_ORG = ""
            CUSTOMER_NUMBER = $Order.StoreCustomerNumber
            BOOKED_FLAG = "Y"
            ATTRIBUTE8 = ""
            CREATION_DATE = "sysdate"
            LAST_UPDATE_DATE = "sysdate"
            ORIG_SYS_CUSTOMER_REF = ""
            ORIG_SHIP_ADDRESS_REF = ""
            ORIG_BILL_ADDRESS_REF = ""
            SHIP_TO_CONTACT_REF = ""
            BILL_TO_CONTACT_REF = ""
            GIFT_MESSAGE = ""
            CUSTOMER_REQUESTED_DATE = ""
            CARRIER_NAME = ""
            CARRIER_SERVICE_LEVEL = ""
            CARRIER_RESIDENTIAL_DELIVERY = ""
            ATTRIBUTE6 = ""
            PROCESS_FLAG = "N"
            SOURCE_NAME = "RMS"
            OPERATING_UNIT_NAME = "Tervis Operating Unit"
            CREATED_BY_NAME = "SHOPIFY"
            LAST_UPDATED_BY_NAME = "SHOPIFY"
        }
    }
}

function Convert-TervisShopifyPaymentsToEBSPayment {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order,
        [Parameter(Mandatory)]$ShopName
    )
    process {
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
            $ReceiptMethodId = Get-TervisShopifyReceiptMethod -PaymentTypeCode $PaymentTypeCode

            [PSCustomObject]@{
                # ORDER_SOURCE_ID = "1101"
                ORDER_SOURCE_ID = "1022" # For use during testing payments
                ORIG_SYS_DOCUMENT_REF = $ORIG_SYS_DOCUMENT_REF
                ORIG_SYS_PAYMENT_REF = $Transaction.id
                PAYMENT_TYPE_CODE = $PaymentTypeCode
                PAYMENT_COLLECTION_EVENT = $PaymentCollectionEvent
                CREDIT_CARD_NUMBER = $CreditCardNumber
                CREDIT_CARD_HOLDER_NAME = $CreditCardName
                CREDIT_CARD_CODE = $CreditCardCode
                # CREDIT_CARD_CODE = ''
                CREDIT_CARD_APPROVAL_CODE = $Transaction.authorization
                CREDIT_CARD_APPROVAL_DATE = $CreditCardApprovalDate
                # CHECK_NUMBER = $CheckNumber
                PAYMENT_AMOUNT = $Transaction.amount
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                CREDIT_CARD_EXPIRATION_MONTH = $Transaction.receipt.payment_method_details.card.exp_month
                CREDIT_CARD_EXPIRATION_YEAR = $Transaction.receipt.payment_method_details.card.exp_year
                CREDIT_CARD_PAYMENT_STATUS = ""
                PROCESS_FLAG = "N"
                # SOURCE_NAME = 'SPF-OSP'
                SOURCE_NAME = 'RMS' # For use during testing payments
                OPERATING_UNIT_NAME = "Tervis Operating Unit"
                CREATED_BY_NAME = "SHOPIFY"
                LAST_UPDATED_BY_NAME = "SHOPIFY"
                RECEIPT_METHOD_ID = $ReceiptMethodId
                PAYMENT_TRX_ID = $Transaction.id
            }
        }
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
            "gift_card" { "CASH"; break } # Pending. Need to see if this is the correct gateway.
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
            return $Transaction.id.ToString().PadLeft(12,"0").Substring(0,12) + `
                $Transaction.payment_details.credit_card_number.Substring(15,4)            
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
    switch ($PaymentTypeCode) {
        # "CHECK" { $ReceiptMethodId; break } # This is now handled in EBS
        "CASH" { "8001"; break }
        "CREDIT_CARD" { "11001"; break }
        default {""}

    }
}

function New-EBSOrderLineSubquery {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ConvertedOrder
    )
    # Need something to check we're not writing over the same order again?
    begin {}
    process {       
        $Query = @"
            INTO xxoe_lines_iface_all
            (
                ORDER_SOURCE_ID,
                ORIG_SYS_DOCUMENT_REF,
                ORIG_SYS_LINE_REF,
                ORIG_SYS_SHIPMENT_REF,
                LINE_TYPE,
                INVENTORY_ITEM,
                ORDERED_QUANTITY,
                ORDER_QUANTITY_UOM,
                SHIP_FROM_ORG,
                PRICE_LIST,
                UNIT_LIST_PRICE,
                UNIT_SELLING_PRICE,
                CALCULATE_PRICE_FLAG,
                RETURN_REASON_CODE,
                CUSTOMER_ITEM_ID_TYPE,
                ATTRIBUTE1,
                ATTRIBUTE7,
                ATTRIBUTE13,
                ATTRIBUTE14,
                CREATION_DATE,
                LAST_UPDATE_DATE,
                SUBINVENTORY,
                EARLIEST_ACCEPTABLE_DATE,
                LATEST_ACCEPTABLE_DATE,
                GIFT_MESSAGE,
                SIDE1_COLOR,
                SIDE2_COLOR,
                SIDE1_FONT,
                SIDE2_FONT,
                SIDE1_TEXT1,
                SIDE2_TEXT1,
                SIDE1_TEXT2,
                SIDE2_TEXT2,
                SIDE1_TEXT3,
                SIDE2_TEXT3,
                SIDE1_INITIALS,
                SIDE2_INITIALS,
                PROCESS_FLAG,
                SOURCE_NAME,
                OPERATING_UNIT_NAME,
                CREATED_BY_NAME,
                LAST_UPDATED_BY_NAME,
                ACCESSORY,
                TAX_VALUE
            )
            VALUES
            (
                '$($ConvertedOrder.ORDER_SOURCE_ID)',
                '$($ConvertedOrder.ORIG_SYS_DOCUMENT_REF)',
                '$($ConvertedOrder.ORIG_SYS_LINE_REF)',
                '$($ConvertedOrder.ORIG_SYS_SHIPMENT_REF)',
                '$($ConvertedOrder.LINE_TYPE)',
                '$($ConvertedOrder.INVENTORY_ITEM)',
                $($ConvertedOrder.ORDERED_QUANTITY),
                '$($ConvertedOrder.ORDER_QUANTITY_UOM)',
                '$($ConvertedOrder.SHIP_FROM_ORG)',
                '$($ConvertedOrder.PRICE_LIST)',
                $($ConvertedOrder.UNIT_LIST_PRICE),
                $($ConvertedOrder.UNIT_SELLING_PRICE),
                '$($ConvertedOrder.CALCULATE_PRICE_FLAG)',
                '$($ConvertedOrder.RETURN_REASON_CODE)',
                '$($ConvertedOrder.CUSTOMER_ITEM_ID_TYPE)',
                '$($ConvertedOrder.ATTRIBUTE1)',
                '$($ConvertedOrder.ATTRIBUTE7)',
                '$($ConvertedOrder.ATTRIBUTE13)',
                '$($ConvertedOrder.ATTRIBUTE14)',
                $($ConvertedOrder.CREATION_DATE),
                $($ConvertedOrder.LAST_UPDATE_DATE),
                '$($ConvertedOrder.SUBINVENTORY)',
                '$($ConvertedOrder.EARLIEST_ACCEPTABLE_DATE)',
                '$($ConvertedOrder.LATEST_ACCEPTABLE_DATE)',
                '$($ConvertedOrder.GIFT_MESSAGE)',
                '$($ConvertedOrder.SIDE1_COLOR)',
                '$($ConvertedOrder.SIDE2_COLOR)',
                '$($ConvertedOrder.SIDE1_FONT)',
                '$($ConvertedOrder.SIDE2_FONT)',
                '$($ConvertedOrder.SIDE1_TEXT1)',
                '$($ConvertedOrder.SIDE2_TEXT1)',
                '$($ConvertedOrder.SIDE1_TEXT2)',
                '$($ConvertedOrder.SIDE2_TEXT2)',
                '$($ConvertedOrder.SIDE1_TEXT3)',
                '$($ConvertedOrder.SIDE2_TEXT3)',
                '$($ConvertedOrder.SIDE1_INITIALS)',
                '$($ConvertedOrder.SIDE2_INITIALS)',
                '$($ConvertedOrder.PROCESS_FLAG)',
                '$($ConvertedOrder.SOURCE_NAME)',
                '$($ConvertedOrder.OPERATING_UNIT_NAME)',
                '$($ConvertedOrder.CREATED_BY_NAME)',
                '$($ConvertedOrder.LAST_UPDATED_BY_NAME)',
                '$($ConvertedOrder.ACCESSORY)',
                '$($ConvertedOrder.TAX_VALUE)'
            )

"@
        return $Query
    }

}

function New-EBSOrderLineHeaderSubquery {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ConvertedOrder
    )

    process {
        $Query = @"
        INTO xxoe_headers_iface_all
        (
            ORDER_SOURCE_ID,
            ORIG_SYS_DOCUMENT_REF,
            ORDERED_DATE,
            ORDER_TYPE,
            PRICE_LIST,
            SALESREP,
            PAYMENT_TERM,
            SHIPMENT_PRIORITY_CODE,
            SHIPPING_METHOD_CODE,
            SHIPMENT_PRIORITY,
            SHIPPING_INSTRUCTIONS,
            CUSTOMER_PO_NUMBER,
            SHIP_FROM_ORG,
            SHIP_TO_ORG,
            INVOICE_TO_ORG,
            CUSTOMER_NUMBER,
            BOOKED_FLAG,
            ATTRIBUTE8,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            ORIG_SYS_CUSTOMER_REF,
            ORIG_SHIP_ADDRESS_REF,
            ORIG_BILL_ADDRESS_REF,
            SHIP_TO_CONTACT_REF,
            BILL_TO_CONTACT_REF,
            GIFT_MESSAGE,
            CUSTOMER_REQUESTED_DATE,
            CARRIER_NAME,
            CARRIER_SERVICE_LEVEL,
            CARRIER_RESIDENTIAL_DELIVERY,
            ATTRIBUTE6,
            PROCESS_FLAG,
            SOURCE_NAME,
            OPERATING_UNIT_NAME,
            CREATED_BY_NAME,
            LAST_UPDATED_BY_NAME
        )
        VALUES
        (
            '$($ConvertedOrder.ORDER_SOURCE_ID)',
            '$($ConvertedOrder.ORIG_SYS_DOCUMENT_REF)',
            $($ConvertedOrder.ORDERED_DATE),
            '$($ConvertedOrder.ORDER_TYPE)',
            '$($ConvertedOrder.PRICE_LIST)',
            '$($ConvertedOrder.SALESREP)',
            '$($ConvertedOrder.PAYMENT_TERM)',
            '$($ConvertedOrder.SHIPMENT_PRIORITY_CODE)',
            '$($ConvertedOrder.SHIPPING_METHOD_CODE)',
            '$($ConvertedOrder.SHIPMENT_PRIORITY)',
            '$($ConvertedOrder.SHIPPING_INSTRUCTIONS)',
            '$($ConvertedOrder.CUSTOMER_PO_NUMBER)',
            '$($ConvertedOrder.SHIP_FROM_ORG)',
            '$($ConvertedOrder.SHIP_TO_ORG)',
            '$($ConvertedOrder.INVOICE_TO_ORG)',
            '$($ConvertedOrder.CUSTOMER_NUMBER)',
            '$($ConvertedOrder.BOOKED_FLAG)',
            '$($ConvertedOrder.ATTRIBUTE8)',
            $($ConvertedOrder.CREATION_DATE),
            $($ConvertedOrder.LAST_UPDATE_DATE),
            '$($ConvertedOrder.ORIG_SYS_CUSTOMER_REF)',
            '$($ConvertedOrder.ORIG_SHIP_ADDRESS_REF)',
            '$($ConvertedOrder.ORIG_BILL_ADDRESS_REF)',
            '$($ConvertedOrder.SHIP_TO_CONTACT_REF)',
            '$($ConvertedOrder.BILL_TO_CONTACT_REF)',
            '$($ConvertedOrder.GIFT_MESSAGE)',
            '$($ConvertedOrder.CUSTOMER_REQUESTED_DATE)',
            '$($ConvertedOrder.CARRIER_NAME)',
            '$($ConvertedOrder.CARRIER_SERVICE_LEVEL)',
            '$($ConvertedOrder.CARRIER_RESIDENTIAL_DELIVERY)',
            '$($ConvertedOrder.ATTRIBUTE6)',
            '$($ConvertedOrder.PROCESS_FLAG)',
            '$($ConvertedOrder.SOURCE_NAME)',
            '$($ConvertedOrder.OPERATING_UNIT_NAME)',
            '$($ConvertedOrder.CREATED_BY_NAME)',
            '$($ConvertedOrder.LAST_UPDATED_BY_NAME)'
        )

"@
        return $Query
    }
}

function New-EBSOrderLinePaymentSubquery {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ConvertedPayment
    )

    process {
        $Query = @"
        INTO xxoe_payments_iface_all
        (
            ORDER_SOURCE_ID,
            ORIG_SYS_DOCUMENT_REF,
            ORIG_SYS_PAYMENT_REF,
            PAYMENT_TYPE_CODE,
            PAYMENT_COLLECTION_EVENT,
            CREDIT_CARD_NUMBER,
            CREDIT_CARD_HOLDER_NAME,
            CREDIT_CARD_CODE,
            CREDIT_CARD_APPROVAL_CODE,
            CREDIT_CARD_APPROVAL_DATE,
            CHECK_NUMBER,
            PAYMENT_AMOUNT,
            CREATION_DATE,
            LAST_UPDATE_DATE,
            CREDIT_CARD_EXPIRATION_MONTH,
            CREDIT_CARD_EXPIRATION_YEAR,
            CREDIT_CARD_PAYMENT_STATUS,
            PROCESS_FLAG,
            SOURCE_NAME,
            OPERATING_UNIT_NAME,
            CREATED_BY_NAME,
            LAST_UPDATED_BY_NAME,
            RECEIPT_METHOD_ID,
            PAYMENT_TRX_ID
        )
        VALUES
        (
            $($ConvertedPayment.ORDER_SOURCE_ID),
            '$($ConvertedPayment.ORIG_SYS_DOCUMENT_REF)',
            '$($ConvertedPayment.ORIG_SYS_PAYMENT_REF)',
            '$($ConvertedPayment.PAYMENT_TYPE_CODE)',
            '$($ConvertedPayment.PAYMENT_COLLECTION_EVENT)',
            '$($ConvertedPayment.CREDIT_CARD_NUMBER)',
            '$($ConvertedPayment.CREDIT_CARD_HOLDER_NAME)',
            '$($ConvertedPayment.CREDIT_CARD_CODE)',
            '$($ConvertedPayment.CREDIT_CARD_APPROVAL_CODE)',
            $($ConvertedPayment.CREDIT_CARD_APPROVAL_DATE),
            '$($ConvertedPayment.CHECK_NUMBER)',
            $($ConvertedPayment.PAYMENT_AMOUNT),
            $($ConvertedPayment.CREATION_DATE),
            $($ConvertedPayment.LAST_UPDATE_DATE),
            '$($ConvertedPayment.CREDIT_CARD_EXPIRATION_MONTH)',
            '$($ConvertedPayment.CREDIT_CARD_EXPIRATION_YEAR)',
            '$($ConvertedPayment.CREDIT_CARD_PAYMENT_STATUS)',
            '$($ConvertedPayment.PROCESS_FLAG)',
            '$($ConvertedPayment.SOURCE_NAME)',
            '$($ConvertedPayment.OPERATING_UNIT_NAME)',
            '$($ConvertedPayment.CREATED_BY_NAME)',
            '$($ConvertedPayment.LAST_UPDATED_BY_NAME)',
            '$($ConvertedPayment.RECEIPT_METHOD_ID)',
            '$($ConvertedPayment.PAYMENT_TRX_ID)'
        )

"@
        return $Query
    }
}

function New-TervisShopifyOrderObject {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        # Initial order object
        $OrderObject = $Order| New-TervisShopifyOrderObjectBase

        # Order lines conversion, for both sales and refunds
        $OrderObject.LineItems = $Order | New-TervisShopifyOrderObjectLines
        # TODO
        # - Add payments section
        # - Add special order functionality

        return $OrderObject
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
        switch ($LineItem.sku) {
            # Substitutes custom shipping SKU in Shopify with the "FREIGHT" EBS item number
            "Shipping-Standard"     { $NewSKU = "1097271"; break }
            "Shipping-Overnight"    { $NewSKU = "1097271"; break }
            "Shipping-Extended"     { $NewSKU = "1097271"; break }
            Default { $NewSku = $LineItem.sku}
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
                # SHIP_FROM_ORG = "'ORG'" # Commenting per drew's suggestion. "It's not a thing"
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
            }
            Customer = [PSCustomObject]@{}
            LineItems = @()
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

        $LineItems += foreach ($Line in $Order.$LineItemType.edges.node) {
            if ($Line.quantity -ne 0) {

                $LineItemCounter++
    
                $Line | Invoke-TervisShopifyLineItemSkuSubstitution
    
                if ($IsRefund) {
                    $LineType = "Tervis Credit Only Line"
                    $InventoryItem = $Line.lineItem.sku
                    $OrderedQuantity = $Line.quantity * -1
                    $UnitListPrice = $Line.priceSet.shopMoney.amount
                    $UnitSellingPrice = $Line.priceSet.shopMoney.amount
                    $ReturnReasonCode = "STORE RETURN"
                    $TaxValue = $Line.totalTaxSet.shopMoney.amount
                } else {
                    $LineType = "Tervis Bill Only with Inv Line"
                    $InventoryItem = $Line.sku
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
                    # TAX_VALUE = $TaxValue
                    TAX_VALUE = "''" # For use in PRD until payments implemented
    
                }
            }
        }

        return $LineItems
    }
}
