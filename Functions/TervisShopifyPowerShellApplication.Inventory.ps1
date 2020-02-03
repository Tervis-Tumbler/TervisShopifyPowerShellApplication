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
        Write-EventLog -LogName Shopify -Source "Inventory Interface" -EntryType Information -EventId 1 `
            -Message "Starting Shopify inventory sync on $NewRecordCount items." 
        Write-Progress -Activity "Shopify interface - inventory update" -CurrentOperation "Setting Shopify locations"
        $Locations = Get-TervisShopifyActiveLocations -ShopName $ShopName  #| select -first 2 #| ? SUBINVENTORY -EQ "FL0"
        #foreach location
            # Get inventory for specific location
            # Create location + inventory object
        # Start parallel jobs with objects + initialization script + creds
        $InitializationExpression = "$ScriptRoot\ParallelInitScript.ps1"
        Write-Progress -Activity "Shopify interface - inventory update" -CurrentOperation "Starting parallel work"
        Start-ParallelWork -Parameters $Locations -OptionalParameters $InitializationExpression,$ShopName -MaxConcurrentJobs 4 -ScriptBlock {
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
            if (
                $Parameter.Subinventory -and
                (Get-TervisShopifyInventoryStagingTableCount -SubinventoryCode $Parameter.Subinventory) -gt 0
            ) {
                $TimePerStore = Measure-Command { # to measure process per store
                $InventoryUpdates = Get-TervisShopifyInventoryStagingTableUpdates -SubinventoryCode $Parameter.Subinventory #| select -first 1000
                Write-Warning "$($Parameter.Subinventory): Testing InventoryUpdates - Count: $($InventoryUpdates.Count)"
                #Get current inventory - Get-ShopifyInventoryAtLocation
                # Measure-Command {
                    $InventoryUpdates | ForEach-Object {
                        try {
                            $SKU = $_.ITEM_NUMBER
                            # Write-Warning "Getting Inventory levels at location"
                            $InventoryItem = Get-ShopifyInventoryLevelAtLocation `
                                -ShopName $ShopName `
                                -SKU $SKU `
                                -LocationId $Parameter.id.split("/")[-1]
    
                            if (
                                $InventoryItem -and
                                $null -eq $InventoryItem.inventoryLevel
                            ) {
                                Invoke-ShopifyInventoryActivate `
                                    -InventoryItemId $InventoryItem.id.split("/")[-1] `
                                    -LocationId $Parameter.id.split("/")[-1] `
                                    -ShopName $ShopName | 
                                    Out-Null
                            }
                        } catch {
                            Write-EventLog -LogName Shopify -Source "Inventory Interface" -EntryType Warning -EventId 2 `
                                -Message "Could not get inventory item information for item #$SKU at $($Parameter.name). Reason:`n$_`n$($_.InvocationInfo.PositionMessage)"
                        }
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
                # Write-Host "$($Parameter.Subinventory): Time to complete query generation: $TimePerStore seconds" -BackgroundColor DarkGray -ForegroundColor Cyan
                Write-EventLog -LogName Shopify -Source "Inventory Interface" -EntryType Information -EventId 1 -Message @"
Inventory processed for `"$($Parameter.Name)`" in $TimePerStore seconds.
Inventory items adjusted: $($InventoryToBeAdjusted.Count)
Inventory items already in sync: $($InventoryAlreadySynced.Count)
Inventory items in EBS but not Shopify: $($InventoryThatErroredOut.Count)
Total inventory items: $($InventoryUpdates.Count)
"@
            }
            elseif (-not $Parameter.Subinventory) {
                Write-EventLog -LogName Shopify -Source "Inventory Interface" -EntryType Warning -EventId 2 `
                    -Message "No subinventory code found for `"$($Parameter.Name)`". Check the LocationDefinition.csv and make sure it is current."            
            }
            else {
                Write-Warning "$($Parameter.Subinventory): No new records"
                Write-EventLog -LogName Shopify -Source "Inventory Interface" -EntryType Information -EventId 1 `
                -Message "No inventory changes found for `"$($Parameter.Name)`"."            

            }
        }
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
        ORDER BY on_hand_qty DESC
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
        # $LogFile = "C:\Logs\ShopifyInventory.log"
        # $ObjectBackup = "C:\Logs\InventorySyncObjects"
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
                $ErrMsg = "$($Response.data.inventoryBulkAdjustQuantityAtLocation.userErrors)`n$($Response.errors)"
            }
        } catch {
            $Synced = "N"
            $ErrMsg = $_
        }        

        $Queries += [PSCustomObject]@{
            Query = $Query
            EBSItemNumbers = $EBSItemNumbers
            SubinventoryCode = $SubinventoryCode
            Synced = $Synced
            ErrorMessage = $ErrMsg
        }
    }
    end {
#         $DateTime = (Get-Date).ToString()
#         $Queries | ForEach-Object {
#             @"
# Time: $DateTime
# Synced: $Synced
# SubinventoryCode: $($_.SubinventoryCode)
# EBSItemNumbers: $($_.EBSItemNumbers)
# Query:
# $($_.Query)


# "@
#         } | Out-File -FilePath $LogFile -Append
        
#         $ObjectBackupFull = "$($ObjectBackup)_$($Queries[0].SubinventoryCode).xml"
#         Export-Clixml -InputObject $Queries -Path $ObjectBackupFull -Force
        $Queries | Where-Object Synced -EQ "N" | ForEach-Object {
            Write-EventLog -LogName Shopify -Source "Inventory Interface" -EntryType Warning -EventId 2 -Message @"
Could not sync $($_.EBSItemNumbers.Count) inventory items to Shopify.

Error:
$($_.ErrorMessage)

Inventory Items:
$(($_.EBSItemNumbers) -join "`n")

Query:
$($_.Query)
"@
        }
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

    # Export-Clixml -InputObject $InventoryArray -Path "C:\Logs\InventoryAlreadySynced_$SubinventoryCode.xml" -Force
}
