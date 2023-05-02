function Find-TervisShopifyEBSOrderNumberAndOrigSysDocumentRef {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$SearchTerm,
        [Parameter(Mandatory)][ValidateSet("order_number","orig_sys_document_ref")]$Column
    )
    begin {
        $BaseQuery = "select order_number, orig_sys_document_ref from apps.oe_order_headers_all "
    }
    process {
        if ($SearchTerm -match "%") {
             $Operator = "LIKE"
        } else {
            $Operator = "="
        }

        $Query = $BaseQuery + "where $Column $Operator '$SearchTerm'"
        Invoke-EBSSQL -SQLCommand $Query
    }
}

function Get-TervisShopifyEBSOrderNumberFromShopifyOrderID {
    param (
        $OrderID
    )
    Find-TervisShopifyEBSOrderNumberAndOrigSysDocumentRef -Column orig_sys_document_ref -SearchTerm "%$OrderID%"
}

function Invoke-TervisShopifyReprocessBTO {
    param (
        $OrderID
    )
    $ShopifyOrder = Get-ShopifyOrder -ShopName tervisstore -OrderId $OrderID
    $Order = Get-TervisShopifyOrdersForImport -ShopName tervisstore -Orders $ShopifyOrder
    $IsBTO = $Order | Test-TervisShopifyBuildToOrder
    if ($IsBTO) {
        $OrderBTO = $Order | ConvertTo-TervisShopifyOrderBTO
        $OrderObject = $OrderBTO | New-TervisShopifyBuildToOrderObject
        $ParameterizedOrderObject = $OrderObject | ConvertTo-TervisShopifyEBSParameterizedValues
        $EBSQueryBTO = $ParameterizedOrderObject.OrderObject | Convert-TervisShopifyOrderObjectToEBSQuery
        $text = $OrderObject | ConvertTo-JsonEx
        Read-Host "$text`n`nContinue?"
        if (-not (Test-TervisShopifyEBSOrderExists -Order $OrderBTO)) {
            Invoke-EBSSQL -SQLCommand $EBSQueryBTO -Parameters $ParameterizedOrderObject.Parameters
        } else {
            Write-Warning "BTO already in EBS"
        }
    } else {
        Write-Warning "No BTO detected"
    }

}

function Invoke-TervisShopifyReprocessOrder {
    param (
        $OrderID,
        $ShopName = "tervisstore"
    )
    $ShopifyOrder = Get-ShopifyOrder -ShopName $ShopName -OrderId $OrderID
    $Order = Get-TervisShopifyOrdersForImport -ShopName $ShopName -Orders $ShopifyOrder
    try {
        if (
            -not $Order.StoreCustomerNumber -or
            -not $Order.Subinventory -or
            -not $Order.ReceiptMethodId
        ) {throw "Location information incomplete. Please update LocationDefinition.csv."}
        $OrderObject = $Order | New-TervisShopifyOrderObject -ShopName $ShopName
        $ParameterizedOrderObject = $OrderObject | ConvertTo-TervisShopifyEBSParameterizedValues
        $EBSQuery = $ParameterizedOrderObject.OrderObject | Convert-TervisShopifyOrderObjectToEBSQuery
        $text = $OrderObject | ConvertTo-JsonEx
        Read-Host "$text`n`nContinue?"
        if ((Test-TervisShopifyEBSOrderExists -Order $Order)) {
            Write-Warning "Order already exists in EBS. Skipping."
        } else {
            Invoke-EBSSQL -SQLCommand $EBSQuery -Parameters $ParameterizedOrderObject.Parameters -ErrorAction Stop
        }
        $Order | Set-ShopifyOrderTag -ShopName $ShopName -AddTag "ImportedToEBS" | Out-Null
    } catch {
        throw "Something went wrong importing Shopify order #$($Order.legacyResourceId). Reason:`n$_`n$($_.InvocationInfo.PositionMessage)" 
    }
}

function Add-TervisShopifySpecialOrderAttributes {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    $LineItems = $Order.lineItems.edges.node
    $Index = 0
    $Choices = @()
    
    $Lines = foreach ($Line in $LineItems) {
        $Choices += $Index
        $SpecialOrder = if ( ($Line.customAttributes | Where-Object key -eq isSpecialOrder).value -eq "true" ) {"Y"}
        $SpecialOrderQty = if ( ($Line.customAttributes | Where-Object key -eq specialOrderQuantity)) {
            ($Line.customAttributes | Where-Object key -eq specialOrderQuantity).value
        } else { "X" }
        [PSCustomObject]@{
            Index = $Index
            SKU = $Line.sku
            Qty = $Line.quantity
            SpecialOrder = $SpecialOrder
            SpecialOrderQty = $SpecialOrderQty
        }
        $Index++
    }
    $Lines | Format-Table *

    do {
        try {
            [int]$SelectionInput = Read-Host "Select line to add Special Order property"
            if ($SelectionInput -in $Choices) {
                $Selection = $SelectionInput
            } else {
                Write-Warning "Enter a number from 0 to $($Index - 1)"
            }
        } catch {
            Write-Warning "Enter a number"
        }
    } while ($null -eq $Selection)

    do {
        $Max = $LineItems[$Selection].quantity
        try {
            [int]$QuantityInput = Read-Host "Enter quantity to special order"
            if ($QuantityInput -gt 0 -and $QuantityInput -le $Max) {
                $QuantityToSpecialOrder = $QuantityInput
            } else {
                Write-Warning "Enter a number from 1 to $Max"
            }
        } catch {
            Write-Warning "Enter a number"
        }
    } while (-not $QuantityToSpecialOrder)

    
    $SpecialOrderAttributes = [PSCustomObject]@{
        key = "isSpecialOrder"
        value = "true"
    }, [PSCustomObject]@{
        key = "specialOrderQuantity"
        value = $QuantityToSpecialOrder
    }, [PSCustomObject]@{
        key = "specialOrderNote"
        value = ""
    }
    
    $Node = $Order.lineItems.edges[$Selection].node
    $Node.customAttributes = [array]$Node.customAttributes + $SpecialOrderAttributes

}

# function Add-TervisShopifyPersonalizationLineItem {
#     param (
#         [Parameter(Mandatory,ValueFromPipeline)]$Order
#     )
#     $LineItems = $Order.lineItems.edges.node
#     $Index = 0
#     $Choices = @()
    
#     $Lines = foreach ($Line in $LineItems) {
#         $Choices += $Index
#         $SpecialOrder = if ( ($Line.customAttributes | Where-Object key -eq isSpecialOrder).value -eq "true" ) {"Y"}
#         $SpecialOrderQty = if ( ($Line.customAttributes | Where-Object key -eq specialOrderQuantity)) {
#             ($Line.customAttributes | Where-Object key -eq specialOrderQuantity).value
#         } else { "X" }
#         [PSCustomObject]@{
#             Index = $Index
#             SKU = $Line.sku
#             Qty = $Line.quantity
#             SpecialOrder = $SpecialOrder
#             SpecialOrderQty = $SpecialOrderQty
#         }
#         $Index++
#     }
#     $Lines | Format-Table *

#     do {
#         try {
#             [int]$SelectionInput = Read-Host "Select line to add Special Order property"
#             if ($SelectionInput -in $Choices) {
#                 $Selection = $SelectionInput
#             } else {
#                 Write-Warning "Enter a number from 0 to $($Index - 1)"
#             }
#         } catch {
#             Write-Warning "Enter a number"
#         }
#     } while ($null -eq $Selection)

#     do {
#         $Max = $LineItems[$Selection].quantity
#         try {
#             [int]$QuantityInput = Read-Host "Enter quantity to special order"
#             if ($QuantityInput -gt 0 -and $QuantityInput -le $Max) {
#                 $QuantityToSpecialOrder = $QuantityInput
#             } else {
#                 Write-Warning "Enter a number from 1 to $Max"
#             }
#         } catch {
#             Write-Warning "Enter a number"
#         }
#     } while (-not $QuantityToSpecialOrder)

    
#     $SpecialOrderAttributes = [PSCustomObject]@{
#         key = "isSpecialOrder"
#         value = "true"
#     }, [PSCustomObject]@{
#         key = "specialOrderQuantity"
#         value = $QuantityToSpecialOrder
#     }, [PSCustomObject]@{
#         key = "specialOrderNote"
#         value = ""
#     }
    
#     $Node = $Order.lineItems.edges[$Selection].node
#     $Node.customAttributes = [array]$Node.customAttributes + $SpecialOrderAttributes
# }


