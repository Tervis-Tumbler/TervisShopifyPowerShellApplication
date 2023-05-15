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
                $Description = $ProductRecord | New-TervisShopifyItemDescription
            } else {
                $Title = $ProductRecord.ITEM_DESCRIPTION
            }
            $ImageURLResolved = Invoke-TervisShopifyResolveEBSImageURL -EBSImageURL $ProductRecord.IMAGE_URL
            $IsOnline = if ($ProductRecord.WEB_PRIMARY_NAME -and $ImageURLResolved) { $true } else { $false }
            $IsTaxable = $ProductRecord | Get-TervisShopifyProductIsTaxable
            $FoundShopifyProduct = Find-ShopifyProduct -ShopName $ShopName -SKU $ProductRecord.Item_Number -MetafieldNamespace tervis
            if ($FoundShopifyProduct.count -gt 1) {throw "Duplicate items found. Cannot update item number $($ProductRecord.Item_Number)"}
            $NewOrUpdatedProduct = if ($FoundShopifyProduct) {
                    [array]$Metafields = $FoundShopifyProduct.metafields.edges.node
                    if (-not $Metafields) {
                        $Metafields = [PSCustomObject]@{
                            namespace = "tervis"
                            key = "ebsDescription"
                            value = $ProductRecord.ITEM_DESCRIPTION
                            type = "string"
                        }
                    }

                    Update-ShopifyProduct -ShopName $ShopName `
                        -Id $FoundShopifyProduct.id `
                        -Title $Title `
                        -Description $Description `
                        -Handle $ProductRecord.ITEM_NUMBER `
                        -Sku $ProductRecord.ITEM_NUMBER `
                        -VariantGID $FoundShopifyProduct.variants.edges.node.id `
                        -Barcode $ProductRecord.UPC `
                        -InventoryPolicy "DENY" `
                        -Tracked true `
                        -InventoryManagement SHOPIFY `
                        -Price $ProductRecord.ITEM_PRICE `
                        -ImageURL $ImageURLResolved `
                        -Vendor "Tervis" `
                        -Metafields $Metafields
                } else {
                    New-ShopifyProduct -ShopName $ShopName `
                        -Title $Title `
                        -Description $Description `
                        -Handle $ProductRecord.ITEM_NUMBER `
                        -Sku $ProductRecord.ITEM_NUMBER `
                        -Barcode $ProductRecord.UPC `
                        -InventoryPolicy "DENY" `
                        -Tracked true `
                        -InventoryManagement SHOPIFY `
                        -Price $ProductRecord.ITEM_PRICE `
                        -ImageURL $ImageURLResolved `
                        -Vendor "Tervis" `
                        -Taxable $IsTaxable `
                        -MetafieldEBSDescription $ProductRecord.ITEM_DESCRIPTION
                }

            if (-not $NewOrUpdatedProduct) { throw ($NewOrUpdatedProduct.errors.message -join "`n") }
            
            # Publish item to POS channel
            $ShopifyRESTProduct = @{id = $NewOrUpdatedProduct.id -replace "[^0-9]"}
            Set-ShopifyRestProductChannel -ShopName $ShopName -Products $ShopifyRESTProduct -Channel global | Out-Null
            
            # Set Online/Offline tags
            Set-TervisShopifyProductOnlineTag -ShopName $ShopName -ShopifyGID $NewOrUpdatedProduct.id -IsOnline $IsOnline -DesignCollection $ProductRecord.DESIGN_COLLECTION

            # Temporary fix until Shopify adds special tax to Branson store location
            $BrasonTaxOverrideCollection = Find-TervisShopifyCollection -ShopName $ShopName -CollectionName "Branson-Tax-Override"
            if ($BrasonTaxOverrideCollection) {
                Add-TervisShopifyProductToCollection -ShopName $ShopName -ShopifyCollectionGID $BrasonTaxOverrideCollection.id -ShopifyProductGIDs $NewOrUpdatedProduct.id | Out-Null
            }

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

function Invoke-TervisShopifyResolveEBSImageURL {
    param (
        $EBSImageURL
    )
    
    if (-not $EBSImageURL) { return }
    $EBSImageURL = $EBSImageURL -replace 'HIGH PERFORMANCE LID','SSWMBLID-PER'
    $EBSImageURL = $EBSImageURL -replace 'DELUXE SPOUT LID','SSWMBLID-DEL'
    $FullURL = "http://images.tervis.com/is/image/$EBSImageURL"
    try {
        Invoke-WebRequest -Uri $FullURL | Out-Null
    } catch {
        return
    }
    return $FullURL 
}

function Set-TervisShopifyProductOnlineTag {
    param (
        [Parameter(Mandatory)]$ShopName,
        [Parameter(Mandatory)]$ShopifyGID,
        $DesignCollection,
        [Parameter(Mandatory)][bool]$IsOnline
    )
    if ($IsOnline) {
        $AddTag = "Online",$DesignCollection | Where-Object {$_ -ne $null}
        $RemoveTag = "Offline"
    } else {
        $AddTag = "Offline",$DesignCollection | Where-Object {$_ -ne $null}
        $RemoveTag = "Online"
    }
    Add-ShopifyTag -ShopName $ShopName -ShopifyGid $ShopifyGID -Tags $AddTag
    Remove-ShopifyTag -ShopName $ShopName -ShopifyGid $ShopifyGID -Tags $RemoveTag
}

function Get-TervisShopifyProductIsTaxable {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ProductRecord
    )
    process {
        if (
            $ProductRecord.ITEM_DESCRIPTION -match "PERS FEE" -or
            $ProductRecord.ITEM_DESCRIPTION -match "GIFT CARD"
        ) {
            return "false"
        } else {
            return "true"
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
                ,design_collection
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
            ,web_primary_name
            ,web_secondary_name
            ,design_collection
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

# Online Store stuff

function Invoke-TervisShopifyItemCollectionSync {
    param (
        [Parameter(Mandatory)]$ShopName,
        $Products,
        $AvailableItems,
        $ProductsLookupTable,
        $AvailableItemsWithGID
    )
    Write-Progress -Activity "Shopify Item Collection Sync" -CurrentOperation "Getting Shopify Products"
    if (-not $Products) { $Products = Get-ShopifyRestProductsAll -ShopName $ShopName }

    # Get items from EBS
    Write-Progress -Activity "Shopify Item Collection Sync" -CurrentOperation "Getting Available Items"
    if (-not $AvailableItems) { $AvailableItems = Get-TervisShopifyItemsAvailableInEBS }
    Write-Progress -Activity "Shopify Item Collection Sync" -CurrentOperation "Getting Collections"
    $Collections = Get-TervisShopifyEBSDesignCollections

    # Build lookup table
    if (-not $ProductsLookupTable) {
        $i = 0
        $total = $Products.Count
        $ProductsLookup = @{}
        foreach ($Product in $Products) {
            Write-Progress -Activity "Shopify Item Collection Sync" -CurrentOperation "Creating Product Lookup Table" -PercentComplete $($i/$total) -Status "$($i/100) of $total`: $($Product.title)"
            $SKU = $Product.variants[0].sku
            $ProductsLookup.Add($SKU,$Product)
            $i += 100
        }
    }
    
    # Add GIDs to items
    if (-not $AvailableItemsWithGID) {
        $i = 0
        $total = $AvailableItems.Count
        foreach ($Item in $AvailableItems) {
            Write-Progress -Activity "Shopify Item Collection Sync" -CurrentOperation "Adding GID to Available Items" -PercentComplete $($i/$total) -Status "Getting GID for $($Item.ITEM_NUMBER)"
            # $ShopifyGID = $Products | Where-Object {$_.variants[0].sku -eq $Item.ITEM_NUMBER} | Select-Object -ExpandProperty admin_graphql_api_id
            $ShopifyGID = $ProductsLookupTable[$Item.ITEM_NUMBER].admin_graphql_api_id
            $i += 50; Write-Progress -Activity "Shopify Item Collection Sync" -CurrentOperation "Adding GID to Available Items" -PercentComplete $($i/$total) "Adding $ShopifyGID to $($Item.ITEM_NUMBER)"
            $Item | Add-Member -MemberType NoteProperty -Name ShopifyGID -Value $ShopifyGID
            $i += 50
        }
    } else {
        $AvailableItems = $AvailableItemsWithGID
    }
    # $AvailableItems | Export-Clixml -Path .\AvailableItemsWithGID.xml


    
    # Add items to collections
    $i = 0
    $total = $Collections.Count
    foreach ($Collection in $Collections) {
        Write-Progress -Activity "Shopify Item Collection Sync" -Status "Finding Shopify Collection" -PercentComplete $($i/$total) -CurrentOperation "Collection: $Collection"
        # Add error checking for null responses
        $ShopifyCollection = Find-TervisShopifyCollection -ShopName $ShopName -CollectionName $Collection
        $i += 33; Write-Progress -Activity "Shopify Item Collection Sync" -Status "Getting Items belonging to Collection" -PercentComplete $($i/$total) -CurrentOperation "Collection: $Collection"
        $CollectionItems = $AvailableItems | Where-Object design_collection -eq $Collection | Where-Object ShopifyGID -ne $null # HMM.... there shouldn't be nulls, I think
        $i += 33; Write-Progress -Activity "Shopify Item Collection Sync" -Status "Generating Add-TervisShopifyProductToCollection argument objects" -PercentComplete $($i/$total) -CurrentOperation "Collection: $Collection"
        # Add-TervisShopifyProductToCollection -ShopName $ShopName -ShopifyCollectionGID $ShopifyCollection.id -ShopifyProductGIDs $CollectionItems.ShopifyGID
        @{
            ShopName = $ShopName
            ShopifyCollectionGID = $ShopifyCollection.id
            ShopifyProductGIDs = $CollectionItems.ShopifyGID
        }
        $i += 34
    }
}

function Add-TervisShopifyCollections {
    param (
        [Parameter(Mandatory)]$ShopName
    )
    $CollectionNames = Get-TervisShopifySuperCollectionName -Collection *
    foreach ($Collection in $CollectionNames) {
        $CollectionHandle = $Collection.Replace(" ","-")
        $Mutation = @"
            mutation {
                  collectionCreate(input: {
                    title: "$Collection"
                    handle: "$CollectionHandle"
                    ruleSet: {
                        appliedDisjunctively: false
                        rules: [
                          {
                            column: VARIANT_INVENTORY
                            relation: GREATER_THAN
                            condition: "3"
                          },
                          {
                            column: TAG
                            relation: EQUALS
                            condition: "$Collection"
                          },
                          {
                            column: TAG
                            relation: EQUALS
                            condition: "Online"
                          }
                        ]
                      }
                  
                  }) {
                    collection {
                      id
                    }
                    userErrors {
                      field
                      message
                    }
                  }
                }
"@
        try {
            $Response = Invoke-ShopifyAPIFunction -ShopName $ShopName -Body $Mutation
            if ($Response.data.collectionCreate.userErrors) { throw $Response.data.collectionCreate.userErrors.message }
            if (-not $Response.data.collectionCreate.collection.id) { throw "No Collection ID returned."}
            Write-Output "$Collection`: Created successfully."
        } catch {
            Write-Warning "$Collection`: Could not create collection. $_"
        }
    }
}

function Get-TervisShopifyEBSDesignCollections {
    $Query = @"
        SELECT UNIQUE(design_collection)
        FROM xxtrvs.xxtrvs_store_item_price_intf 
        WHERE design_collection IS NOT NULL  
"@
    Invoke-EBSSQL -SQLCommand $Query | Select-Object -ExpandProperty DESIGN_COLLECTION
}

function Find-TervisShopifyCollection {
    param (
        [Parameter(Mandatory)]$ShopName,
        [Parameter(Mandatory,ValueFromPipeline)]$CollectionName
    )
    $CollectionHandle = $CollectionName.Replace(" ","-")
    $Query = @"
        {
            collectionByHandle(handle:"$CollectionHandle") {
                id
            }
        }
"@
    try {
        $Response = Invoke-ShopifyAPIFunction -ShopName $ShopName -Body $Query
        return $Response.data.collectionByHandle       
    } catch {
        throw $_
    }
}

function Add-TervisShopifyProductToCollection {
    param (
        [Parameter(Mandatory)]$ShopName,
        [Parameter(Mandatory)]$ShopifyCollectionGID,
        [Parameter(Mandatory)][array]$ShopifyProductGIDs
    )
    Write-Warning "Collection '$ShopifyCollectionGID' - Adding $($ShopifyProductGIDs.count) products"
    $ProductLimit = 250
    for ($i = 0; $i -lt $ShopifyProductGIDs.Count; $i += $ProductLimit) {
        Write-Warning "Collection '$ShopifyCollectionGID' - Adding $i through $($i + $ProductLimit)"
        [array]$ProductGIDsToAdd = $ShopifyProductGIDs | Select-Object -First $ProductLimit -Skip $i
        $JoinedProductGIDs = $ProductGIDsToAdd -join '","'
        $Mutation = @"
            mutation {
                collectionAddProducts(
                    id: "$ShopifyCollectionGID"
                    productIds: ["$JoinedProductGIDs"]
                ) {
                    collection {
                        id 
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
"@
        # try {
        #     $Response = Invoke-ShopifyAPIFunction -ShopName $ShopName -Body $Mutation
        # }
    
        Invoke-ShopifyAPIFunction -ShopName $ShopName -Body $Mutation
    }
}

function Add-TervisShopifyItemCollectionTag {
    param (
        [Parameter(Mandatory)]$ShopName,
        [Parameter(Mandatory)][array]$EBSItemNumbers
    )
        $i = 0
        $total = $EBSItemNumbers.Count
    foreach ($EBSItemNumber in $EBSItemNumbers) {
        Write-Progress -Activity "Adding Collection Tags" -CurrentOperation $EBSItemNumber -PercentComplete $($i/$total)
        try {
            $EBSItem = Get-TervisShopifyEBSItem -EBSItemNumber $EBSItemNumber
            $Collection = $EBSItem | Select-Object -ExpandProperty DESIGN_COLLECTION
            $SuperCollection = Get-TervisShopifySuperCollectionName -Collection $Collection
            $ShopifyGID = Find-ShopifyProduct -SKU $EBSItemNumber -ShopName $ShopName | Select-Object -ExpandProperty id
            
            $ImageURLResolved = Invoke-TervisShopifyResolveEBSImageURL -EBSImageURL $EBSItem.IMAGE_URL
            $IsOnline = if ($EBSItem.WEB_PRIMARY_NAME -and $ImageURLResolved) { $true } else { $false }
            
            if (-not $ShopifyGID) { throw "Item not on Shopify" }
            
            # Add-ShopifyTag -ShopName $ShopName -ShopifyGid $ShopifyGID -Tags $Collection,$SuperCollection,"Online"
            Set-TervisShopifyProductOnlineTag -ShopName $ShopName -ShopifyGID $ShopifyGID -IsOnline $IsOnline -DesignCollection Collection
            if ($SuperCollection) {Add-ShopifyTag -ShopName $ShopName -ShopifyGid $ShopifyGID -Tags $SuperCollection}
        } catch { 
            Write-Warning "$EBSItemNumber`: Could not add tag. Reason: $_"
        }
        $i += 100
    }
}

function Get-TervisShopifyEBSItem {
    param (
        [Parameter(Mandatory)]$EBSItemNumber
    )
    $Query = "SELECT * FROM xxtrvs.xxtrvs_store_item_price_intf WHERE item_number = '$EBSItemNumber'"
    Invoke-EBSSQL $Query
}
