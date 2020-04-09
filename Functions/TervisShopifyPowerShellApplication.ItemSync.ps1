function Invoke-TervisShopifyInterfaceItemUpdate {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment,
        $ScriptRoot
    )
    
    Write-Progress -Activity "Syncing products to Shopify" -CurrentOperation "Setting environment variables"
    Set-TervisEBSEnvironment -Name $Environment 2> $null
    Set-TervisShopifyEnvironment -Environment $Environment

    $ShopName = Get-TervisShopifyEnvironmentShopName -Environment $Environment

    [array]$NewRecords = Get-TervisShopifyItemStagingTableUpdates | ? {$_.ITEM_NUMBER[-1] -ne "P"} # Temporary fix
    # $NewRecordCount = Get-TervisShopifyItemStagingTableCount
    $NewRecordCount = $NewRecords.Count # Temporary fix
    if ($NewRecordCount -gt 0) {
        Write-Progress -Activity "Syncing products to Shopify" -CurrentOperation "Getting product records"
        Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Information -EventId 1 `
            -Message "Starting Shopify sync on $NewRecordCount items." 
        # $NewRecords = Get-TervisShopifyItemStagingTableUpdates # Temporary fix
        $Queues = Split-ArrayIntoArrays -InputObject $NewRecords -NumberOfArrays 4
        $InitializationExpression = "$ScriptRoot\ParallelInitScript.ps1"
        $isSuccessful = @()
        $isSuccessful += Start-ParallelWork -Parameters $Queues -OptionalParameters $InitializationExpression,$ShopName -ShowProgress -ScriptBlock {
            param (
                $Parameter,
                $OptionalParameters
            )
            if (-not $Parameter) {return}
            & $OptionalParameters[0] 2> $null
            $ShopName = $OptionalParameters[1]
            $Result = $Parameter | ForEach-Object {
                if ($_.ITEM_STATUS -in "Active","DTCDeplete","Hold","Pending") {
                    $_ | Invoke-TervisShopifyAddOrUpdateProduct -ShopName $ShopName
                } else {
                    $_ | Invoke-TervisShopifyRemoveProduct -ShopName $ShopName
                }
            }
            return $Result
        }
        Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Information -EventId 1 `
            -Message "Completed Shopify item sync.`nSuccessful: $($isSuccessful.Where({$_ -eq $true}).count)`nFailed: $($isSuccessful.Where({$_ -eq $false}).count)"
    }
}

function Invoke-TervisShopifyAddOrUpdateProduct {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ProductRecord,
        [Parameter(Mandatory)]$ShopName
    )
    process {    
        try {
            if ($ProductRecord.web_primary_name) {
                $Title = $ProductRecord.web_primary_name | ConvertTo-ShopifyFriendlyString
                $Description = $ProductRecord.web_secondary_name | ConvertTo-ShopifyFriendlyString
            } else {
                $Title = $ProductRecord.ITEM_DESCRIPTION
            }
            $FoundShopifyProduct = Find-ShopifyProduct -ShopName $ShopName -SKU $ProductRecord.Item_Number
            if ($FoundShopifyProduct.count -gt 1) {throw "Duplicate items found. Cannot update item number $($ProductRecord.Item_Number)"}
            $NewOrUpdatedProduct = if ($FoundShopifyProduct) {
                    Update-ShopifyProduct -ShopName $ShopName `
                        -Id $FoundShopifyProduct.id `
                        -Title $Title `
                        -Description $Description `
                        -Handle $ProductRecord.ITEM_NUMBER `
                        -Sku $ProductRecord.ITEM_NUMBER `
                        -VariantGID $FoundShopifyProduct.variants.edges.node.id `
                        -Barcode $ProductRecord.UPC `
                        # -InventoryPolicy "CONTINUE" ` # Commenting out only during COVID
                        -Tracked true `
                        -InventoryManagement SHOPIFY `
                        -Price $ProductRecord.ITEM_PRICE `
                        -ImageURL "http://images.tervis.com/is/image/$($ProductRecord.IMAGE_URL)" `
                        -Vendor "Tervis"
                } else {
                    New-ShopifyProduct -ShopName $ShopName `
                        -Title $Title `
                        -Description $Description `
                        -Handle $ProductRecord.ITEM_NUMBER `
                        -Sku $ProductRecord.ITEM_NUMBER `
                        -Barcode $ProductRecord.UPC `
                        # -InventoryPolicy "CONTINUE" ` # Commenting out only during COVID
                        -Tracked true `
                        -InventoryManagement SHOPIFY `
                        -Price $ProductRecord.ITEM_PRICE `
                        -ImageURL "http://images.tervis.com/is/image/$($ProductRecord.IMAGE_URL)" `
                        -Vendor "Tervis"
                }
            # Publish item to POS channel
            $ShopifyRESTProduct = @{id = $NewOrUpdatedProduct.id -replace "[^0-9]"}
            Set-ShopifyRestProductChannel -ShopName $ShopName -Products $ShopifyRESTProduct -Channel global | Out-Null

            # Write back to EBS staging table
            Set-TervisShopifyItemStagingTableUpdateFlag -EbsItemNumber $NewOrUpdatedProduct.variants.edges.node.inventoryItem.sku
            return $true
        } catch {
            # Write-Warning "$($_.ITEM_NUMBER) could not be created on Shopify"
            Write-Warning $_
            Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Warning -EventId 3 `
                -Message "Could not sync item $($ProductRecord.Item_Number) `nReason:`n$_`n$($_.InvocationInfo.PositionMessage)"
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
            Write-EventLog -LogName Shopify -Source "Item Interface" -EntryType Warning -EventId 3 `
                -Message "Could not sync item $($ProductRecord.Item_Number) `nReason:`n$_`n$($_.InvocationInfo.PositionMessage)"
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
                ,web_primary_name
                ,web_secondary_name
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

function Invoke-TervisShopifyItemPrune {
    param (
        [Parameter(Mandatory)]$ShopName
    )

}

function Get-TervisShopifyItemsAvailableInEBS {
    $Query = @"
        SELECT
            item_id
            ,item_number
            ,item_description
            ,item_status
            ,item_price
            ,price_list_name
            ,upc
            ,image_url
        FROM xxtrvs.xxtrvs_store_item_price_intf
        WHERE 1 = 1
        AND item_status IN ('Active','DTCDeplete','Hold','Pending')
"@
    Invoke-EBSSQL -SQLCommand $Query
}

function Get-TervisShopifyNumberOfStoresWithItemQtyGreaterThanZero {
    param (
        # [Parameter(Mandatory)]$ShopName,
        [Parameter(Mandatory)]$EbsItemNumber
    )
    $Query = @"
        SELECT count(*)               
        FROM xxtrvs.xxinv_store_ohq
        WHERE 1 = 1
        AND item_number = '$EbsItemNumber'
        AND on_hand_qty > 0
"@
    Invoke-EBSSQL -SQLCommand $Query | Select-Object -ExpandProperty "COUNT(*)"
}

function Get-TervisShopifyItemEBSInventoryOnHandQuantityCount {
    param (
        [Parameter(Mandatory)]$EBSItemNumber
    )
    $Query = @"
        SELECT SUM(on_hand_qty) ALLSTOREONHANDQTY
        FROM xxtrvs.xxinv_store_ohq
        WHERE 1 = 1
        AND item_number = '$EBSItemNumber'
"@
    Invoke-EBSSQL -SQLCommand $Query | Select-Object -ExpandProperty ALLSTOREONHANDQTY
}
