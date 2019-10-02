function Invoke-TervisShopifyPowerShellApplicationProvision {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    Invoke-ApplicationProvision -ApplicationName ShopifyInterface -EnvironmentName $EnvironmentName
    $Nodes = Get-TervisApplicationNode -ApplicationName ShopifyInterface -EnvironmentName $EnvironmentName
    $Nodes | Install-TervisShopifyPowerShellApplicationLog
    $Nodes | Install-TervisShopifyPowerShellApplication_ItemInterface
    $Nodes | Install-TervisShopifyPowerShellApplication_InventoryInterface
}

function Install-TervisShopifyPowerShellApplicationLog {
    param (
        [Parameter(ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(ValueFromPipelineByPropertyName)]$EnvironmentName
    )
    begin {
        $LogName = "Shopify"
        $LogSources = `
            "Item Interface",
            "Sales Interface",
            "Inventory Interface"
    }
    process {
        try {
            New-EventLog -ComputerName $ComputerName -LogName $LogName -Source $LogSources -ErrorAction
        } catch [System.InvalidOperationException] {
            Write-Warning "Log and sources already exist"
        }
    }
}

function Install-TervisShopifyPowerShellApplication_ItemInterface {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]
        [ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    begin {
        $ScheduledTasksCredential = Get-TervisPasswordstatePassword -Guid "eed2bd81-fd47-4342-bd59-b396da75c7ed" -AsCredential
    }
    process {
        $PowerShellApplicationParameters = @{
            ComputerName = $ComputerName
            EnvironmentName = $EnvironmentName
            ModuleName = "TervisShopifyPowerShellApplication"
            TervisModuleDependencies = `
                "WebServicesPowerShellProxyBuilder",
                "TervisPowerShellJobs",
                "PasswordstatePowershell",
                "TervisPasswordstatePowershell",
                "OracleE-BusinessSuitePowerShell",
                "TervisOracleE-BusinessSuitePowerShell",
                "InvokeSQL",
                "TervisMicrosoft.PowerShell.Utility",
                "TervisMicrosoft.PowerShell.Security",
                "ShopifyPowerShell",
                "TervisShopify",
                "TervisShopifyPowerShellApplication"
            NugetDependencies = "Oracle.ManagedDataAccess.Core"
            ScheduledTaskName = "ShopifyItemInterface"
            RepetitionIntervalName = "EveryDayEvery15Minutes"
            CommandString = "Invoke-TervisShopifyInterfaceItemUpdate -Environment $EnvironmentName"
            ScheduledTasksCredential = $ScheduledTasksCredential
        }
        
        Install-PowerShellApplication @PowerShellApplicationParameters
    }
}

function Install-TervisShopifyPowerShellApplication_InventoryInterface {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$ComputerName,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]
        [ValidateSet("Delta","Epsilon","Production")]$EnvironmentName
    )
    begin {
        $ScheduledTasksCredential = Get-TervisPasswordstatePassword -Guid "eed2bd81-fd47-4342-bd59-b396da75c7ed" -AsCredential
    }
    process {
        $PowerShellApplicationParameters = @{
            ComputerName = $ComputerName
            EnvironmentName = $EnvironmentName
            ModuleName = "TervisShopifyPowerShellApplication"
            TervisModuleDependencies = `
                "WebServicesPowerShellProxyBuilder",
                "TervisPowerShellJobs",
                "PasswordstatePowershell",
                "TervisPasswordstatePowershell",
                "OracleE-BusinessSuitePowerShell",
                "TervisOracleE-BusinessSuitePowerShell",
                "InvokeSQL",
                "TervisMicrosoft.PowerShell.Utility",
                "TervisMicrosoft.PowerShell.Security",
                "ShopifyPowerShell",
                "TervisShopify",
                "TervisShopifyPowerShellApplication"
            NugetDependencies = "Oracle.ManagedDataAccess.Core"
            ScheduledTaskName = "ShopifyInventoryInterface"
            RepetitionIntervalName = "EveryDayAt3am"
            CommandString = "Invoke-TervisShopifyInterfaceInventoryUpdate -Environment $EnvironmentName -ScriptRoot $PSScriptRoot"
            ScheduledTasksCredential = $ScheduledTasksCredential
        }
        
        Install-PowerShellApplication @PowerShellApplicationParameters
        
        $PowerShellApplicationParameters.CommandString = @"
Set-TervisEBSEnvironment -Name $Environment 2> `$null
Set-TervisShopifyEnvironment -Environment $Environment
"@
        $PowerShellApplicationParameters.ScriptFileName = "ParallelInitScript.ps1"
        $PowerShellApplicationParameters.Remove("RepetitionIntervalName")
        $PowerShellApplicationParameters.Remove("ScheduledTasksCredential")
        Install-PowerShellApplicationFiles @PowerShellApplicationParameters -ScriptOnly

    }
}

function Get-TervisShopifyEnvironmentShopName {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )

    switch ($Environment) {
        "Delta" {"DLT-TervisStore"; break}
        "Epsilon" {"tervisteststore01"; break}
        "Production" {"tervisstore"; break}
        default {throw "Environment not recognized"}
    }
}

function Invoke-TervisShopifyInterfaceItemUpdate {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )
    
    Write-Progress -Activity "Syncing products to Shopify" -CurrentOperation "Setting environment variables"
    Set-TervisEBSEnvironment -Name $Environment 2> $null
    Set-TervisShopifyEnvironment -Environment $Environment

    $ShopName = Get-TervisShopifyEnvironmentShopName -Environment $Environment
    $Locations = Get-ShopifyLocation -ShopName $ShopName -LocationName *

    # $ProductUpdateScriptBlock = {
    #     param($Parameter,$OptionalParameters)
    #     Set-TervisShopifyEnvironment -Environment dev
    #     Set-TervisEBSEnvironment -Name Delta
    #     if ($Parameter.ITEM_STATUS -in "Active","DTCDeplete") {
    #         $Parameter | Invoke-TervisShopifyAddOrUpdateProduct -ShopName $OptionalParameters.ShopName -Locations $OptionalParameters.Locations
    #     } else {
    #         $Parameter | Invoke-TervisShopifyRemoveProduct -ShopName $OptionalParameters.ShopName
    #     }
    # }
    # $MaxConcurrentRequests = 3

    $NewRecordCount = Get-TervisShopifyItemStagingTableCount
    if ($NewRecordCount -gt 0) {
        Write-Progress -Activity "Syncing products to Shopify" -CurrentOperation "Getting product records"
        Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Information -EventId 1 `
            -Message "Starting Shopify sync on $NewRecordCount items." 
        $i = 0
        $NewRecords = Get-TervisShopifyItemStagingTableUpdates
        # Start-ParallelWork -ScriptBlock $ProductUpdateScriptBlock -Parameters $NewRecords -OptionalParameters $OtherParams -MaxConcurrentJobs $MaxConcurrentRequests
        $NewRecords | ForEach-Object {
            $i++
            Write-Progress -Activity "Syncing products to Shopify" -Status "$i of $NewRecordCount" `
                -PercentComplete ($i * 100 / $NewRecordCount) -CurrentOperation "Processing EBS item #$($_.ITEM_NUMBER)" -SecondsRemaining (($NewRecordCount - $i) * 4)
            $isSuccessful = @()
            $isSuccessful += if ($_.ITEM_STATUS -in "Active","DTCDeplete") {
                $_ | Invoke-TervisShopifyAddOrUpdateProduct -ShopName $ShopName -Locations $Locations
            } else {
                $_ | Invoke-TervisShopifyRemoveProduct -ShopName $ShopName
            }
        }
        Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Information -EventId 1 `
            -Message "Completed Shopify item sync.`nSuccessful: $($isSuccessful.Where({$_ -eq $true}).count)`nFailed: $($isSuccessful.Where({$_ -eq $false}).count)"
    }
}

function Invoke-TervisShopifyAddOrUpdateProduct {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ProductRecord,
        [Parameter(Mandatory)]$Locations,
        [Parameter(Mandatory)]$ShopName
    )
    process {    
        try {
            $FoundShopifyProduct = Find-ShopifyProduct -ShopName $ShopName -SKU $ProductRecord.Item_Number
            if ($FoundShopifyProduct.count -gt 1) {throw "Duplicate items found. Cannot update item number $($ProductRecord.Item_Number)"}
            $NewOrUpdatedProduct = if ($FoundShopifyProduct) {
                    Update-ShopifyProduct -ShopName $ShopName `
                        -Id $FoundShopifyProduct.id `
                        -Title $ProductRecord.ITEM_DESCRIPTION `
                        -Handle $ProductRecord.ITEM_NUMBER `
                        -Sku $ProductRecord.ITEM_NUMBER `
                        -VariantGID $FoundShopifyProduct.variants.edges.node.id `
                        -Barcode $ProductRecord.UPC `
                        -InventoryPolicy "CONTINUE" `
                        -Tracked true `
                        -InventoryManagement SHOPIFY `
                        -Price $ProductRecord.ITEM_PRICE `
                        -ImageURL "http://images.tervis.com/is/image/$($ProductRecord.IMAGE_URL)" `
                        -Vendor "Tervis"
                } else {
                    New-ShopifyProduct -ShopName $ShopName `
                        -Title $ProductRecord.ITEM_DESCRIPTION `
                        -Handle $ProductRecord.ITEM_NUMBER `
                        -Sku $ProductRecord.ITEM_NUMBER `
                        -Barcode $ProductRecord.UPC `
                        -InventoryPolicy "CONTINUE" `
                        -Tracked true `
                        -InventoryManagement SHOPIFY `
                        -Price $ProductRecord.ITEM_PRICE `
                        -ImageURL "http://images.tervis.com/is/image/$($ProductRecord.IMAGE_URL)" `
                        -Vendor "Tervis"
                }
            $ShopifyRESTProduct = @{id = $NewOrUpdatedProduct.id -replace "[^0-9]"}
            $ShopifyInventoryItemId = $NewOrUpdatedProduct.variants.edges.node.inventoryItem.id -replace "[^0-9]"
            # Publish item to POS channel
            Set-ShopifyRestProductChannel -ShopName $ShopName -Products $ShopifyRESTProduct -Channel global | Out-Null
            # Make item available at all locations -replace "[^0-9]"
            $InventoryItemLocations = Get-ShopifyInventoryItemLocations -ShopName $ShopName -InventoryItemId $ShopifyInventoryItemId
            $MissingLocationIDs = $Locations | 
                Where-Object Name -NotIn $InventoryItemLocations.Name |
                Select-Object -ExpandProperty id | 
                Get-ShopifyIdFromShopifyGid

            $MissingLocationIDs | ForEach-Object {
                Invoke-ShopifyInventoryActivate -InventoryItemId $ShopifyInventoryItemId -LocationId $_ -ShopName $ShopName | Out-Null
            }

            # Write back to EBS staging table
            Set-TervisShopifyItemStagingTableUpdateFlag -EbsItemNumber $NewOrUpdatedProduct.variants.edges.node.inventoryItem.sku
            return $true
        } catch {
            # Write-Warning "$($_.ITEM_NUMBER) could not be created on Shopify"
            Write-Warning $_
            Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Warning -Message "Could not sync item $($ProductRecord.Item_Number) `nReason:`n$_"
            return $false
        }
    }
}

function Invoke-TervisShopifyRemoveProduct {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ProductRecord,
        [Parameter(Mandatory)]$ShopName
    )
    process {
        try {
            $ShopifyProduct = Find-ShopifyProduct -ShopName $ShopName -SKU $ProductRecord.Item_Number
            if ($ShopifyProduct) {
                Remove-ShopifyProduct -GlobalId $ShopifyProduct.id -ShopName $ShopName | Out-Null
            }
            Set-TervisShopifyItemStagingTableUpdateFlag -EbsItemNumber $ProductRecord.ITEM_NUMBER
            return $true
        } catch {
            Write-Warning $_
            Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Warning -Message "Could not sync item $($ProductRecord.Item_Number) `nReason:`n$_"
            return $false
        }

    }
}

function Get-TervisShopifyItemStagingTableCount {
    $Query = @"
        SELECT count(*) 
        FROM xxtrvs.xxtrvs_store_item_price_intf
        WHERE 1 = 1
        AND interfaced_flag = 'N'
"@
    Invoke-EBSSQL -SQLCommand $Query | Select-Object -ExpandProperty "COUNT(*)"
}

function Get-TervisShopifyItemStagingTableUpdates {
    $Query = @"
        SELECT  item_id
                ,item_number
                ,item_description
                ,item_status
                ,item_price
                ,price_list_name
                ,upc
                ,image_url
        FROM xxtrvs.xxtrvs_store_item_price_intf
        WHERE 1 = 1
        AND interfaced_flag = 'N'
        ORDER BY 1
"@
    Invoke-EBSSQL -SQLCommand $Query 
}

function Set-TervisShopifyItemStagingTableUpdateFlag {
    param (
        [Parameter(Mandatory)]$EbsItemNumber
    )
    $Query = @"
        UPDATE xxtrvs.xxtrvs_store_item_price_intf
        SET interfaced_flag = 'Y', interfaced_date = sysdate
        WHERE item_number = '$EbsItemNumber'
"@
    Invoke-EBSSQL -SQLCommand $Query
}

function Invoke-TervisShopifyInterfaceSalesImport {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )

    Write-Progress -Activity "Shopify Sales Batch Interface" -CurrentOperation "Setting environment variables"
    Set-TervisEBSEnvironment -Name $Environment 2> $null
    Set-TervisShopifyEnvironment -Environment $Environment
    
    $ShopName = Get-TervisShopifyEnvironmentShopName -Environment $Environment
    
    try {
        Write-Progress -Activity "Shopify Sales Batch Interface" -CurrentOperation "Getting orders"
        $ShopifyOrders = Get-TervisShopifyOrdersNotTaggedWithEBS -ShopName $ShopName
        Write-EventLog -LogName Shopify -Source "Sales Interface" -EntryType Information -EventId 1 `
                -Message "Starting Shopify sync on $NewRecordCount items." 
        Write-Progress -Activity "Shopify Sales Batch Interface" -CurrentOperation "Converting orders to EBS format"
        $ConvertedOrderLines = $ShopifyOrders | Convert-TervisShopifyOrderToEBSOrderLines
        $ConvertedOrderHeaders = $ShopifyOrders | Convert-TervisShopifyOrderToEBSOrderLineHeader
        $Subqueries = $ConvertedOrderLines | New-EBSOrderLineSubquery
        $Subqueries += $ConvertedOrderHeaders | New-EBSOrderLineHeaderSubquery
        Write-Progress -Activity "Shopify Sales Batch Interface" -CurrentOperation "Sending orders to EBS"
        $Subqueries | Invoke-EBSSubqueryInsert
        Write-Progress -Activity "Shopify Sales Batch Interface" -CurrentOperation "Tagging orders sent to EBS"
        $ShopifyOrders | Set-ShopifyOrderTag -ShopName $ShopName -AddTag "SentToEBS"
    } catch {
        # Need to improve error handling, possibly isolate the order tagging
        # process so that an internet hiccup doesn't retrigger all the orders
        # at a later time.
        Write-EventLog -LogName Shopify -Source "Sales Interface" -EntryType Error -EventId 1 `
                -Message "Something went wrong. Reason:`n$_" 
        $_
    }
}

function Convert-TervisShopifyOrderToEBSOrderLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $LocationDefinition = Get-TervisShopifyLocationDefinition -City $Order.physicalLocation.address.city
        # May need to adjust this for local time based on location 
        # $DateString = Get-Date -Date ([datetime]::Parse($Order.createdat).toLocalTime()) -Format yyyyMMdd_HHmmss_ffff
        $OrderId = $Order.id | Get-ShopifyIdFromShopifyGid
        $StoreNumber = $LocationDefinition.RMSStoreNumber
        $ORIG_SYS_DOCUMENT_REF = "$StoreNumber-$OrderId"
        $OrderLineNumber = 0
        $Order.lineItems.edges.node | ForEach-Object {
            $OrderLineNumber++
            [PSCustomObject]@{
                ORDER_SOURCE_ID = "1022" # For use during testing payments
                ORIG_SYS_DOCUMENT_REF = $ORIG_SYS_DOCUMENT_REF
                ORIG_SYS_LINE_REF = "$OrderLineNumber"
                ORIG_SYS_SHIPMENT_REF = ""
                LINE_TYPE = "Tervis Bill Only with Inv Line"
                INVENTORY_ITEM = "$($_.sku)"
                ORDERED_QUANTITY = $_.quantity
                ORDER_QUANTITY_UOM = "EA"
                SHIP_FROM_ORG = "STO"
                PRICE_LIST = ""
                UNIT_LIST_PRICE = $($_.originalUnitPriceSet.shopMoney.amount)
                UNIT_SELLING_PRICE = $($_.originalUnitPriceSet.shopMoney.amount)
                CALCULATE_PRICE_FLAG = "P"
                RETURN_REASON_CODE = ""
                CUSTOMER_ITEM_ID_TYPE = ""
                ATTRIBUTE1 = ""
                ATTRIBUTE7 = ""
                ATTRIBUTE13 = ""
                ATTRIBUTE14 = ""
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                SUBINVENTORY = $LocationDefinition.Subinventory
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
                TAX_VALUE = $_.taxLines.priceSet.shopMoney.amount | Measure-Object -Sum | Select-Object -ExpandProperty Sum
            }
        }
    }
}

function Convert-TervisShopifyOrderToEBSOrderLineHeader {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $LocationDefinition = Get-TervisShopifyLocationDefinition -City $Order.physicalLocation.address.city
        $OrderId = $Order.id | Get-ShopifyIdFromShopifyGid
        $StoreNumber = $LocationDefinition.RMSStoreNumber
        $StoreCustomerNumber = $LocationDefinition.CustomerNumber
        $ORIG_SYS_DOCUMENT_REF = "$StoreNumber-$OrderId"

        [PSCustomObject]@{
            ORDER_SOURCE_ID = "1022" # For use during testing payments
            ORIG_SYS_DOCUMENT_REF = $ORIG_SYS_DOCUMENT_REF
            ORDERED_DATE = "TO_DATE('$($Order.createdAt)', 'YYYY-MM-DD`"T`"HH24:MI:SS`"Z`"')"
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
            CUSTOMER_NUMBER = $StoreCustomerNumber
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
        $Transaction = $Order | Get-ShopifyRestOrderTransactionDetail -ShopName $ShopName
        $LocationDefinition = Get-TervisShopifyLocationDefinition -City $Order.physicalLocation.address.city
        $OrderId = $Order.id | Get-ShopifyIdFromShopifyGid
        $StoreNumber = $LocationDefinition.RMSStoreNumber
        # $StoreCustomerNumber = $LocationDefinition.CustomerNumber
        $ORIG_SYS_DOCUMENT_REF = "$StoreNumber-$OrderId"
        $PaymentTypeCode = $Transaction | Get-TervisShopifyPaymentTypeCode
        $PaymentCollectionEvent = $Transaction | Get-TervisShopifyPaymentCollectionEvent
        $CreditCardNumber = $Transaction | New-TervisShopifyCCDummyNumber
        $CreditCardApprovalDate = if ($CreditCardNumber) {$Transaction.processed_at | ConvertTo-TervisShopifyOracleSqlUtcDateString}


        [PSCustomObject]@{
            # ORDER_SOURCE_ID = "1101"
            ORDER_SOURCE_ID = "1022" # For use during testing payments
            ORIG_SYS_DOCUMENT_REF = $ORIG_SYS_DOCUMENT_REF
            ORIG_SYS_PAYMENT_REF = $Transaction.id
            PAYMENT_TYPE_CODE = $PaymentTypeCode
            PAYMENT_COLLECTION_EVENT = $PaymentCollectionEvent
            CREDIT_CARD_NUMBER = $CreditCardNumber
            CREDIT_CARD_HOLDER_NAME = ""
            CREDIT_CARD_CODE = 'UNKNOWN'
            CREDIT_CARD_APPROVAL_CODE = $Transaction.authorization
            CREDIT_CARD_APPROVAL_DATE = $CreditCardApprovalDate
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

function ConvertTo-TervisShopifyOracleSqlUtcDateString {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$DateTime
    )
    $UtcDateTimeString = (Get-Date $DateTime).ToUniversalTime().GetDateTimeFormats("u")
    $OracleSqlToDateFunction = "TO_DATE('$UtcDateTimeString', 'YYYY-MM-DD HH24:MI:SS`"Z`"')"
    return $OracleSqlToDateFunction
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
            LAST_UPDATED_BY_NAME
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
            $($ConvertedPayment.PAYMENT_AMOUNT),
            $($ConvertedPayment.CREATION_DATE),
            $($ConvertedPayment.LAST_UPDATE_DATE),
            $($ConvertedPayment.CREDIT_CARD_EXPIRATION_MONTH),
            $($ConvertedPayment.CREDIT_CARD_EXPIRATION_YEAR),
            '$($ConvertedPayment.CREDIT_CARD_PAYMENT_STATUS)',
            '$($ConvertedPayment.PROCESS_FLAG)',
            '$($ConvertedPayment.SOURCE_NAME)',
            '$($ConvertedPayment.OPERATING_UNIT_NAME)',
            '$($ConvertedPayment.CREATED_BY_NAME)',
            '$($ConvertedPayment.LAST_UPDATED_BY_NAME)'
        )
"@
        return $Query
    }
}

function Invoke-EBSSubqueryInsert {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Subquery
    )
    begin {
        $FinalQuery = "INSERT ALL"
    }
    process {
        $FinalQuery += "`n$Subquery"
    }
    end {
        $FinalQuery += "`nSELECT 1 FROM DUAL"
        Invoke-EBSSQL -SQLCommand $FinalQuery
    }
}

function Invoke-TervisShopifyInterfaceInventoryUpdate {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment,
        $ScriptRoot
    )
    
    Write-Progress -Activity "Shopify interface - inventory update" -CurrentOperation "Setting environment variables"
    Set-TervisEBSEnvironment -Name $Environment 2> $null
    Set-TervisShopifyEnvironment -Environment $Environment

    $ShopName = Get-TervisShopifyEnvironmentShopName -Environment $Environment

    Write-Progress -Activity "Shopify interface - inventory update" -CurrentOperation "Getting inventory update count"
    $NewRecordCount = Get-TervisShopifyInventoryStagingTableCount
    if ($NewRecordCount -gt 0) {
        # Get active locations with relevant information, like subinventory code
        Write-Progress -Activity "Shopify interface - inventory update" -CurrentOperation "Setting Shopify locations"
        $Locations = Get-TervisShopifyActiveLocations -ShopName $ShopName  #| select -first 2 #| ? SUBINVENTORY -EQ "FL0"
        #foreach location
            # Get inventory for specific location
            # Create location + inventory object
        # Start parallel jobs with objects + initialization script + creds
        $InitializationExpression = "$ScriptRoot\ParallelInitScript.ps1"
        Write-Progress -Activity "Shopify interface - inventory update" -CurrentOperation "Starting parallel work"
        Start-ParallelWork -MaxConcurrentJobs 4 -ScriptBlock {
            param (
                $Parameter,
                $OptionalParameters
            )
            & $OptionalParameters[0] 2> $null
            $ShopName = $OptionalParameters[1]
            # Get-ShopifyLocation -ShopName ospreystoredev -LocationName $Parameter.name
            # Get-TervisShopifyInventoryStagingTableUpdates -SubinventoryCode FL1 # 25 seconds for 100 records
            # $InventoryUpdates
            Write-Warning "$($Parameter.Subinventory): Getting inventory updates"
            if ((Get-TervisShopifyInventoryStagingTableCount -SubinventoryCode $Parameter.Subinventory) -gt 0) {
                $TimePerStore = Measure-Command { # to measure process per store
                $InventoryUpdates = Get-TervisShopifyInventoryStagingTableUpdates -SubinventoryCode $Parameter.Subinventory #| select -first 1000
                Write-Warning "$($Parameter.Subinventory): Testing InventoryUpdates - Count: $($InventoryUpdates.Count)"
                #Get current inventory - Get-ShopifyInventoryAtLocation
                # Measure-Command {
                    $InventoryUpdates | ForEach-Object {
                        # Write-Warning "Getting Inventory levels at location"
                        $InventoryItem = Get-ShopifyInventoryLevelAtLocation `
                            -ShopName $ShopName `
                            -SKU $_.ITEM_NUMBER `
                            -LocationId $Parameter.id.split("/")[-1]
                        # Write-Warning "Calculating difference"
                        $Difference = if ($InventoryItem) {
                            $_.ON_HAND_QTY - $InventoryItem.inventoryLevel.available
                        } else {
                            # Write-Error "InventoryInterface: Error getting Shopify inventory level. SKU: $($_.ITEM_NUMBER), Subinventory: $($_.SUBINVENTORY_CODE)"
                            "E"
                        }
                        # Write-Warning "Adding members"
                        $_ | Add-Member -MemberType NoteProperty -Name Difference -Value $Difference -Force
                        $_ | Add-Member -MemberType NoteProperty -Name ShopifyGID -Value $InventoryItem.id -Force
                    }
                # } # 30 seconds for 100 records
    
                # Need to filter on difference 
                Write-Warning "$($Parameter.Subinventory): Creating arrays"
                [array]$InventoryAlreadySynced = $InventoryUpdates | Where-Object Difference -EQ 0 # Maybe log? Can probably be ignored
                [array]$InventoryToBeAdjusted = $InventoryUpdates | Where-Object {$_.Difference -ne 0 -and $_.Difference -ne "E"}
                [array]$InventoryThatErroredOut = $InventoryUpdates | Where-Object Difference -EQ "E" # Need function to handle errored Inventory
                
                Write-Warning "$($Parameter.Subinventory): Inventory to be adjusted: $($InventoryToBeAdjusted.Count)"
                Write-Warning "$($Parameter.Subinventory): Inventory that had no matching Shopify item: $($InventoryThatErroredOut.Count)"
                Write-Warning "$($Parameter.Subinventory): Inventory already in sync: $($InventoryAlreadySynced.Count)"
                if ($InventoryToBeAdjusted) {
                    Write-Warning "$($Parameter.Subinventory): Creating query objects"
                    [array]$QueryObjects = New-TervisShopifyInventoryBulkAdjustQueryObject -InventoryArray $InventoryToBeAdjusted -LocationGID $Parameter.id
                    
                    Write-Warning "$($Parameter.Subinventory): Query Objects created: $($QueryObjects.count)"
                    $QueryObjects | Sync-TervisShopifyInventoryFromQueryObject -ShopName $ShopName
                }
                if ($InventoryAlreadySynced) {
                    Set-TervisShopifyInventoryStagingTableUpdateFlagOnSyncedInventory -InventoryArray $InventoryAlreadySynced -SubinventoryCode $Parameter.Subinventory
                }
                if ($InventoryThatErroredOut) {
                    Export-Clixml -Force -InputObject $InventoryThatErroredOut -Path "C:\Logs\InventoryErrored_$($Parameter.Subinventory).xml"
                }
                } | Select-Object -ExpandProperty TotalSeconds # end measure-command
                Write-Host "$($Parameter.Subinventory): Time to complete query generation: $TimePerStore seconds" -BackgroundColor DarkGray -ForegroundColor Cyan
            }
            else {Write-Warning "$($Parameter.Subinventory): No new records"}
        } -Parameters $Locations -OptionalParameters $InitializationExpression,$ShopName
    }

}

function Get-TervisShopifyInventoryStagingTableCount {
    param (
        $SubinventoryCode
    )
    $Query = @"
        SELECT count(*) 
        FROM xxtrvs.xxinv_store_ohq
        WHERE 1 = 1
        AND interfaced_flag = 'N'
        $(if ($SubinventoryCode) {"AND subinventory_code = '$SubinventoryCode'"})
"@
    Invoke-EBSSQL -SQLCommand $Query | Select-Object -ExpandProperty "COUNT(*)"
}

function Get-TervisShopifyInventoryStagingTableUpdates {
    param (
        $SubinventoryCode
    )

    $Query = @"
        SELECT  item_number
                ,subinventory_code
                ,on_hand_qty               
        FROM xxtrvs.xxinv_store_ohq
        WHERE 1 = 1
        AND interfaced_flag = 'N'
        $(if ($SubinventoryCode) {"AND subinventory_code = '$SubinventoryCode'"})
"@
    Invoke-EBSSQL -SQLCommand $Query 
}

function New-TervisShopifyInventoryBulkAdjustQueryObject {
    param (
        [Parameter(Mandatory)][array]$InventoryArray,
        [Parameter(Mandatory)][string]$LocationGID,
        $QueryLimit = 100 # this is a limit imposed by Shopify
    )
    # Write-Warning "Initializing GraphQL query templates"
    $GraphQLHeader = @"
mutation {
    inventoryBulkAdjustQuantityAtLocation (
        inventoryItemAdjustments:
            [
"@
    
    $GraphQLFooter = {
        param ($LocationGID)
        $EncodedGID = ConvertTo-Base64 -String $LocationGID
    @"
            ],
        locationId: "$EncodedGID"
    ) {
        userErrors {
            field
            message
        }
    }
}
"@
    } 
    
    $InventoryBulkAdjustEntry = {
        param (
            $InventoryItemGID,
            $Delta
        )
        $EncodedGID = ConvertTo-Base64 -String $InventoryItemGID
    @"

                {
                    inventoryItemId: "$EncodedGID"
                    availableDelta: $Delta
                },

"@
    }
    
    # Write-Warning "Testing InventoryArray - Count: $($InventoryArray.count)"
    $BuiltQueries = @()
    
    for ($i = 0; $i -lt $InventoryArray.Count; $i += $QueryLimit) {
        # Write-Warning "Iteration: $i"
        $Query = $GraphQLHeader
        $InventorySubset = $InventoryArray[$i..($i + $QueryLimit - 1)]
        # Write-Warning "InventorySubset - Count: $($InventorySubset.Count)"
        $Query += $InventorySubset | ForEach-Object {
            $InventoryBulkAdjustEntry.Invoke($_.ShopifyGID,$_.Difference)
        }
        $Query += $GraphQLFooter.Invoke($LocationGID)
        $BuiltQueries += [PSCustomObject]@{
            Query = $Query
            EBSItemNumbers = [array]$InventorySubset.ITEM_NUMBER
            SubinventoryCode = $InventorySubset[0].SUBINVENTORY_CODE
        }
    }

    # Write-Warning "Testing BuiltQueries - Count: $($BuiltQueries.Count)"
    # Write-Warning "Testing BuiltQuery - EbsItemNumbers `n$($BuiltQueries[0].EbsItemNumbers)"
    return $BuiltQueries
}

function Sync-TervisShopifyInventoryFromQueryObject {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$Query,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)][array]$EBSItemNumbers,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$SubinventoryCode,
        [Parameter(Mandatory)]$ShopName
    )
    begin {
        $LogFile = "C:\Logs\ShopifyInventory.log"
        $ObjectBackup = "C:\Logs\InventorySyncObjects"
        $Queries = @()
    }
    process {
        try{
            $Response = Invoke-ShopifyAPIFunction -ShopName $ShopName -Body $Query
            # If no errors, set EBS shopify flag
            if (
                -not $Response.data.inventoryBulkAdjustQuantityAtLocation.userErrors -and
                -not $Response.errors
                ) {
                Set-TervisShopifyInventoryStagingTableUpdateFlag `
                    -EBSItemNumbers $EBSItemNumbers `
                    -SubinventoryCode $SubinventoryCode
                $Synced = "Y"
            } else { # Else, error handling. Log objects?
                $Synced = "N"
            }
        } catch {
            $Synced = "N"
        }        

        $Queries += [PSCustomObject]@{
            Query = $Query
            EBSItemNumbers = $EBSItemNumbers
            SubinventoryCode = $SubinventoryCode
            Synced = $Synced
        }
    }
    end {
        $DateTime = (Get-Date).ToString()
        $Queries | ForEach-Object {
            @"
Time: $DateTime
Synced: $Synced
SubinventoryCode: $($_.SubinventoryCode)
EBSItemNumbers: $($_.EBSItemNumbers)
Query:
$($_.Query)


"@
        } | Out-File -FilePath $LogFile -Append
        
        $ObjectBackupFull = "$($ObjectBackup)_$($Queries[0].SubinventoryCode).xml"
        Export-Clixml -InputObject $Queries -Path $ObjectBackupFull -Force
    }
}

function Set-TervisShopifyInventoryStagingTableUpdateFlag {
    param (
        [Parameter(Mandatory)][array]$EBSItemNumbers,
        [Parameter(Mandatory)]$SubinventoryCode,
        [ValidateSet("Y","N")]$InterfacedFlag = "Y"
    )
    $EBSItemNumberSet = "('$($EBSItemNumbers -join "','")')"
    $Query = @"
        UPDATE xxtrvs.xxinv_store_ohq
        SET interfaced_flag = '$InterfacedFlag', interfaced_date = sysdate
        WHERE 1 = 1
        AND item_number IN $EBSItemNumberSet
        AND subinventory_code = '$SubinventoryCode'
"@
    Invoke-EBSSQL -SQLCommand $Query
}

function Set-TervisShopifyInventoryStagingTableUpdateFlagOnSyncedInventory {
    param (
        [Parameter(Mandatory)][array]$InventoryArray,
        [Parameter(Mandatory)]$SubinventoryCode,
        $QueryLimit = 1000 # EBS max array 
    )

    for ($i = 0; $i -lt $InventoryArray.Count; $i += $QueryLimit) {
        $EBSItemNumbers = $InventoryArray[$i..($i + $QueryLimit - 1)].ITEM_NUMBER
        Set-TervisShopifyInventoryStagingTableUpdateFlag `
            -EBSItemNumbers $EBSItemNumbers `
            -SubinventoryCode $SubinventoryCode
    }

    Export-Clixml -InputObject $InventoryArray -Path "C:\Logs\InventoryAlreadySynced_$SubinventoryCode.xml" -Force
}
