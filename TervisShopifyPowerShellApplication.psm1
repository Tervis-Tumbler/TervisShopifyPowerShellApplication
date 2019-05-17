# TESTING 
function Invoke-TervisShopifyInterfaceItemUpdate {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )

    $ShopNames = @{
        Delta = "ospreystoredev"
        Epsilon = ""
        Production = "tervisteststore01"
    }

    $ShopName = $ShopNames[$Environment]
    $Locations = Get-ShopifyRestLocations -ShopName $ShopName
    Set-TervisEBSEnvironment -Name $Environment
    $ProblemIds = @()

    $NewRecordCount = Get-ShopifyStagingTableCount
    if ($NewRecordCount -gt 0) {
        $NewRecords = Get-ShopifyStagingTableUpdates | select -First 10
        $NewRecords | ForEach-Object {
            try {
                $FoundShopifyProduct = Find-ShopifyProduct -ShopName $ShopName -SKU $_.Item_Number
                $NewOrUpdatedProduct = if ($FoundShopifyProduct) {
                        Update-ShopifyProduct -ShopName $ShopName `
                            -Id $FoundShopifyProduct.id `
                            -Title $_.ITEM_DESCRIPTION `
                            -Handle $_.ITEM_NUMBER `
                            -Sku $_.ITEM_NUMBER `
                            -Barcode $_.UPC `
                            -InventoryPolicy "CONTINUE" `
                            -Tracked true `
                            -InventoryManagement SHOPIFY `
                            -Price $_.ITEM_PRICE `
                            -ImageURL "http://images.tervis.com/is/image/$($_.IMAGE_URL)" `
                            -Vendor "Tervis"
                    } else {
                        New-ShopifyProduct -ShopName $ShopName `
                            -Title $_.ITEM_DESCRIPTION `
                            -Handle $_.ITEM_NUMBER `
                            -Sku $_.ITEM_NUMBER `
                            -Barcode $_.UPC `
                            -InventoryPolicy "CONTINUE" `
                            -Tracked true `
                            -InventoryManagement SHOPIFY `
                            -Price $_.ITEM_PRICE `
                            -ImageURL "http://images.tervis.com/is/image/$($_.IMAGE_URL)" `
                            -Vendor "Tervis"
                    }
                $ShopifyRESTProduct = @{id = $NewOrUpdatedProduct.id -replace "[^0-9]"}
                $ShopifyInventoryItemId = $NewOrUpdatedProduct.variants.edges.node.inventoryItem.id -replace "[^0-9]"
                # Publish item to POS channel
                Set-ShopifyRestProductChannel -ShopName $ShopName -Products $ShopifyRESTProduct -Channel global
                # Make item available at all locations -replace "[^0-9]"
                $InventoryItemLocations = Get-ShopifyInventoryItemLocations -ShopName $ShopName -InventoryItemId $ShopifyInventoryItemId
                $MissingLocations = $Locations | Where-Object Name -NotIn $InventoryItemLocations.Name
                foreach ($LocationId in $MissingLocations.id) {
                    Invoke-ShopifyInventoryActivate -InventoryItemId $ShopifyInventoryItemId -LocationId $LocationId -ShopName $ShopName
                }

                # Write back to EBS staging table
                Set-ShopifyStagingTableUpdateFlag
            } catch {
                # Write-Warning "$($_.ITEM_NUMBER) could not be created on Shopify"
                Write-Error $_
            }
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
"@
    Invoke-EBSSQL -SQLCommand $Query 
}

function Test-ShopifyItemUpdate {} # Return boolean

function Set-ShopifyStagingTableUpdateFlag {}



# FINAL
