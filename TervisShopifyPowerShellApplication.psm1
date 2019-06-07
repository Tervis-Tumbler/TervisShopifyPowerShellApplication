# TESTING 
function Invoke-TervisShopifyInterfaceItemUpdate {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )
    
    Write-Progress -Activity "Syncing products to Shopify" -CurrentOperation "Setting environment variables"
    Set-TervisEBSEnvironment -Name $Environment
    Set-TervisShopifyEnvironment -Environment $Environment

    $ShopNames = @{
        Delta = "ospreystoredev"
        Epsilon = ""
        Production = "tervisteststore01"
    }

    $OtherParams = @{
        ShopName = $ShopNames[$Environment]
        Locations = Get-ShopifyRestLocations -ShopName $ShopNames[$Environment]
    }

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

    $NewRecordCount = Get-ShopifyStagingTableCount
    if ($NewRecordCount -gt 0) {
        $i = 0
        Write-Progress -Activity "Syncing products to Shopify" -CurrentOperation "Getting product records"
        $NewRecords = Get-ShopifyStagingTableUpdates
        # Start-ParallelWork -ScriptBlock $ProductUpdateScriptBlock -Parameters $NewRecords -OptionalParameters $OtherParams -MaxConcurrentJobs $MaxConcurrentRequests
        $NewRecords | ForEach-Object {
            $i++; Write-Progress -Activity "Syncing products to Shopify" -Status "$i of $NewRecordCount" -PercentComplete ($i * 100 / $NewRecordCount) -CurrentOperation "Processing EBS item #$($_.ITEM_NUMBER)" -SecondsRemaining (($NewRecordCount - $i) * 4)
            if ($_.ITEM_STATUS -in "Active","DTCDeplete") {
                $_ | Invoke-TervisShopifyAddOrUpdateProduct -ShopName $OtherParams.ShopName -Locations $OtherParams.Locations
            } else {
                $_ | Invoke-TervisShopifyRemoveProduct -ShopName $OtherParams.ShopName
            }
        }
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
            $NewOrUpdatedProduct = if ($FoundShopifyProduct) {
                    Update-ShopifyProduct -ShopName $ShopName `
                        -Id $FoundShopifyProduct.id `
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
            $MissingLocations = $Locations | Where-Object Name -NotIn $InventoryItemLocations.Name
            foreach ($LocationId in $MissingLocations.id) {
                Invoke-ShopifyInventoryActivate -InventoryItemId $ShopifyInventoryItemId -LocationId $LocationId -ShopName $ShopName | Out-Null
            }

            # Write back to EBS staging table
            Set-ShopifyStagingTableUpdateFlag -EbsItemNumber $NewOrUpdatedProduct.variants.edges.node.inventoryItem.sku
        } catch {
            # Write-Warning "$($_.ITEM_NUMBER) could not be created on Shopify"
            Write-Error $_
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
            Set-ShopifyStagingTableUpdateFlag -EbsItemNumber $ProductRecord.ITEM_NUMBER
        } catch {
            Write-Error $_
        }

    }
}

function Get-ShopifyStagingTableCount {
    $Query = @"
        SELECT count(*) 
        FROM xxtrvs.xxtrvs_store_item_price_intf
        WHERE 1 = 1
        AND interfaced_flag = 'N'
"@
    Invoke-EBSSQL -SQLCommand $Query | Select-Object -ExpandProperty "COUNT(*)"
}

function Get-ShopifyStagingTableUpdates {
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

function Test-ShopifyItemUpdate {} # Return boolean

function Set-ShopifyStagingTableUpdateFlag {
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

function Invoke-TervisShopifyOrderLinesInterface {
    param (
        [Parameter(Mandatory)]$Environment
    )

    Write-Progress -Activity "Syncing products to Shopify" -CurrentOperation "Setting environment variables"
    Set-TervisEBSEnvironment -Name $Environment
    Set-TervisShopifyEnvironment -Environment $Environment

    $ShopNames = @{
        Delta = "ospreystoredev"
        Epsilon = ""
        Production = "tervisteststore01"
    }

    $OtherParams = @{
        ShopName = $ShopNames[$Environment]
        Locations = Get-ShopifyRestLocations -ShopName $ShopNames[$Environment]
    }

    $ShopifyOrders = Get-ShopifyOrders
    $ConvertedOrders = $ShopifyOrders | Convert-TervisShopifyOrderToEBSOrderLines
    $ConvertedOrders | Write-EBSOrderLines
}

function Convert-TervisShopifyOrderToEBSOrderLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    begin {}
    process {
        # May need to adjust this for local time based on location 
        $DateString = Get-Date -Date ([datetime]::Parse($Order.createdat).toLocalTime()) -Format yyyyMMdd_HHmmss
        $StoreNumber = $Order.physicalLocation.name # Will be replaced with function to get actual store number
        $ORIG_SYS_DOCUMENT = "$StoreNumber-$DateString"

        $OrderLineNumber = 0
        $Order.lineItems.edges.node | ForEach-Object {
            $OrderLineNumber++
            [PSCustomObject]@{
                ORIG_SYS_DOCUMENT = $ORIG_SYS_DOCUMENT
                ORIG_SYS_LINE_REF = "$OrderLineNumber"
                ORIG_SYS_SHIPMENT_REF = ""
                LINE_TYPE = "Tervis Bill Only with Inv Line"
                INVENTORY_ITEM = "$($_.sku)"
                ORDERED_QUANTITY = $_.quantity
                ORDER_QUANTITY_UOM = "EA"
                SHIP_FROM_ORG = "STO"
                PRICE_LIST = ""
                UNIT_LIST_PRICE = "$($_.originalUnitPriceSet.shopMoney.amount)"
                UNIT_SELLING_PRICE = "$($_.originalUnitPriceSet.shopMoney.amount)"
                CALCULATE_PRICE_FLAG = "P"
                RETURN_REASON_CODE = ""
                CUSTOMER_ITEM_ID_TYPE = ""
                ATTRIBUTE1 = ""
                ATTRIBUTE7 = ""
                ATTRIBUTE13 = ""
                ATTRIBUTE14 = ""
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                SUBINVENTORY = "" # Needs function to get store code, e.g. FL1, FL2
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
                SOURCE_NAME = "Shopify"
                OPERATING_UNIT_NAME = "Tervis Operating Unit"
                CREATED_BY_NAME = "SHOPIFY"
                LAST_UPDATED_BY_NAME = "SHOPIFY"
                ACCESSORY = ""
            }
        }
    }
}
 
function Write-EBSOrderLines {}