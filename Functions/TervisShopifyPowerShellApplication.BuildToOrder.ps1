function New-TervisShopifyBuildToOrderObject {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $OrderObject = $Order| New-TervisShopifyOrderObjectBase
        $OrderObject | Add-TervisShopifyBuildToOrderHeaderProperties -Order $Order
        $OrderObject.Customer = $Order | New-TervisShopifyBuildToOrderCustomerInfo
        $OrderObject.LineItems = $Order | New-TervisShopifyBuildToOrderLines
        return $OrderObject
    }
}

function Add-TervisShopifyBuildToOrderHeaderProperties {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$OrderObject,
        [Parameter(Mandatory)]$Order
    )
    process {
        $ShipMethodCode = $Order.CustomAttributes.shipMethodCode
        $FreeFreight = $Order.CustomAttributes.freeFreight
        $CustomerName = ("$($Order.CustomAttributes.customerFirstName) $($Order.CustomAttributes.customerLastName)").Trim(" ")

        $OrigSysDocumentRef = $OrderObject.Header.ORIG_SYS_DOCUMENT_REF
        $CustomerPONumber = "'$($OrigSysDocumentRef.Trim("'").Split("-")[1])-$($CustomerName)'"

        $PropertiesToAdd = @{
            SHIPPING_METHOD_CODE = "'$($ShipMethodCode)'"
            CUSTOMER_PO_NUMBER = $CustomerPONumber
            ORIG_SYS_CUSTOMER_REF = $OrigSysDocumentRef
            ORIG_SHIP_ADDRESS_REF = $OrigSysDocumentRef
            SHIP_TO_CONTACT_REF = $OrigSysDocumentRef
            ATTRIBUTE6 = "'$($FreeFreight)'" 
            CUSTOMER_REQUESTED_DATE = "sysdate"
            ORDER_TYPE = "'DTC Sales Order'"
        }
        
        foreach ($Property in $PropertiesToAdd.Keys) {
            $OrderObject.Header | Add-Member -MemberType NoteProperty -Name $Property -Value $PropertiesToAdd[$Property] -Force
        }
    }
}

function New-TervisShopifyBuildToOrderCustomerInfo {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $CustomerName = ("$($Order.CustomAttributes.customerFirstName) $($Order.CustomAttributes.customerLastName)").Trim(" ")
        
        function Test-NullValue {
            param (
                [Parameter(ValueFromPipeline)]$String
            )
            if ($String -eq "''") { throw "Missing shipping address data"}
            $String
        }

        [PSCustomObject]@{
            ORIG_SYS_DOCUMENT_REF = "'$($Order.EBSDocumentReference)'"
            PARENT_CUSTOMER_REF = "'$($Order.StoreCustomerNumber)'"
            PERSON_FIRST_NAME = "'$($Order.CustomAttributes.customerFirstName)'" | Test-NullValue
            PERSON_LAST_NAME = "'$($Order.CustomAttributes.customerLastName)'"
            ADDRESS1 = "'$($Order.CustomAttributes.customerAddress1)'" | Test-NullValue
            ADDRESS2 = "'$($Order.CustomAttributes.customerAddress2)'"
            CITY = "'$($Order.CustomAttributes.customerCity)'" | Test-NullValue
            STATE = "'$($Order.CustomAttributes.customerState)'" | Test-NullValue
            POSTAL_CODE = "'$($Order.CustomAttributes.customerZip)'" | Test-NullValue
            COUNTRY = "'$($Order.CustomAttributes.customerCountryCode)'" | Test-NullValue
            PROCESS_FLAG = "'N'"
            SOURCE_NAME = "'RMS'"
            OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
            CREATED_BY_NAME = "'SHOPIFY'"
            LAST_UPDATED_BY_NAME = "'SHOPIFY'"
            CREATION_DATE = "sysdate"
            # Below only applies to CUSTOMER_TYPE "ORGANIZATION" 
            PARTY_ID = "360580"
            CUSTOMER_TYPE = "'ORGANIZATION'"
            ORGANIZATION_NAME = "'$CustomerName'"
            CUSTOMER_INFO_TYPE_CODE = "'ADDRESS'"
            CUSTOMER_INFO_REF = "'$($Order.EBSDocumentReference)'"
            IS_SHIP_TO_ADDRESS = "'Y'"
            IS_BILL_TO_ADDRESS = "'N'"
            FREIGHT_TERMS = "'$($Order.CustomAttributes.freightTerms)'" | Test-NullValue
            SHIP_METHOD_CODE = "'$($Order.CustomAttributes.shipMethodCode)'" | Test-NullValue
        },
        [PSCustomObject]@{
            ORIG_SYS_DOCUMENT_REF = "'$($Order.EBSDocumentReference)'"
            PARENT_CUSTOMER_REF = "'$($Order.StoreCustomerNumber)'"
            PERSON_FIRST_NAME = "'$($Order.CustomAttributes.customerFirstName)'"
            PERSON_LAST_NAME = "'$($Order.CustomAttributes.customerLastName)'"
            ADDRESS1 = "'$($Order.CustomAttributes.customerAddress1)'"
            ADDRESS2 = "'$($Order.CustomAttributes.customerAddress2)'"
            CITY = "'$($Order.CustomAttributes.customerCity)'"
            STATE = "'$($Order.CustomAttributes.customerState)'"
            POSTAL_CODE = "'$($Order.CustomAttributes.customerZip)'"
            COUNTRY = "'$($Order.CustomAttributes.customerCountryCode)'"
            PROCESS_FLAG = "'N'"
            SOURCE_NAME = "'RMS'"
            OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
            CREATED_BY_NAME = "'SHOPIFY'"
            LAST_UPDATED_BY_NAME = "'SHOPIFY'"
            CREATION_DATE = "sysdate"
            # Below only applies to CUSTOMER_TYPE "ORGANIZATION" 
            PARTY_ID = "360580"
            CUSTOMER_TYPE = "'ORGANIZATION'"
            ORGANIZATION_NAME = "'$CustomerName'"
            CUSTOMER_INFO_TYPE_CODE = "'CONTACT'"
            CUSTOMER_INFO_REF = "'$($Order.EBSDocumentReference)'"
            IS_SHIPPING_CONTACT = "'Y'"
            FREIGHT_TERMS = "'$($Order.CustomAttributes.freightTerms)'"
            SHIP_METHOD_CODE = "'$($Order.CustomAttributes.shipMethodCode)'"
        }
    }
}

function New-TervisShopifyBuildToOrderLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        # Add TervisPropterties to all llne items 
        $Order.lineItems.edges.node | Add-TervisShopifyLineItemProperties
        $Order.lineItems.edges.node | Invoke-TervisShopifyLineItemSkuSubstitution

        $CombinedLineItems = @()
        # Personalization:
        $CombinedLineItems += $Order | Select-TervisShopifyOrderPersonalizationLines
        # Special Order
        $CombinedLineItems += $Order | Select-TervisShopifyOrderSpecialOrderLines
        
        # Now to make the line item objects
        $LineCounter = 0
        $FinalLineItems += foreach ($Line in $CombinedLineItems) {
            $LineCounter++
            $CustomerSuppliedProperties = $Line.TervisProperties | New-TervisShopifyCustomerSuppliedProperties
            if ($Line.TervisProperties.isSpecialOrder -eq "true") {
                $LineNote = "**Special Order**$($Line.TervisProperties.specialOrderNote)"
                $InventoryItem = $Line.sku
            } else {
                if (-not $Line.TervisProperties.RelatedLineItemSKU) { throw "No personalization data on line item" }
                $LineNote = $CustomerSuppliedProperties.CustomerSuppliedDecorationNote
                $InventoryItem = "$($Line.TervisProperties.RelatedLineItemSKU)P"
            }

            [PSCustomObject]@{
                # For EBS
                ORDER_SOURCE_ID = "'1022'"
                ORIG_SYS_DOCUMENT_REF = "'$($Order.EBSDocumentReference)'"
                ORIG_SYS_LINE_REF = "'$($LineCounter)'"
                LINE_TYPE = "'Tervis Bill Only Line'"
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                PROCESS_FLAG = "'N'"
                SOURCE_NAME = "'RMS'"
                OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
                CREATED_BY_NAME = "'SHOPIFY'"
                LAST_UPDATED_BY_NAME = "'SHOPIFY'"
                
                # From Shopify
                INVENTORY_ITEM = "'$InventoryItem'"
                ORDERED_QUANTITY = "$($Line.quantity)"
                UNIT_SELLING_PRICE = "0" # $Line.discountedUnitPriceSet.shopMoney.amount # Might not be relevant
                UNIT_LIST_PRICE = "0"
                
                SIDE1_FONT = "'$($Line.TervisProperties.Side1FontName)'"
                SIDE1_COLOR = "'$($Line.TervisProperties.Side1ColorName)'"
                ATTRIBUTE1 = "'$($CustomerSuppliedProperties.Side1CustomerSupplied)'" # CustomerProvided = PersGrap, Other              
                SIDE1_TEXT1 = "'$($Line.TervisProperties.Side1Line1)'"
                SIDE1_TEXT2 = "'$($Line.TervisProperties.Side1Line2)'"
                SIDE1_TEXT3 = "'$($Line.TervisProperties.Side1Line3)'"
                
                SIDE2_FONT = "'$($Line.TervisProperties.Side2FontName)'"
                SIDE2_COLOR = "'$($Line.TervisProperties.Side2ColorName)'"
                ATTRIBUTE7 = "'$($CustomerSuppliedProperties.Side2CustomerSupplied)'" # CustomerProvided = PersGrap, Other
                SIDE2_TEXT1 = "'$($Line.TervisProperties.Side2Line1)'"
                SIDE2_TEXT2 = "'$($Line.TervisProperties.Side2Line2)'"
                SIDE2_TEXT3 = "'$($Line.TervisProperties.Side2Line3)'"

                ATTRIBUTE14 = "'$LineNote'"
            }
        }
        
        return $FinalLineItems
    }
}

function Add-TervisShopifyLineItemProperties {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$LineItem
    )
    process {
        $LineItemProperties = $LineItem | Convert-TervisShopifyCustomAttributesToObject
        $LineItem | Add-Member -MemberType NoteProperty -Name TervisProperties -Value $LineItemProperties -Force
    }
}

function Select-TervisShopifyOrderSpecialOrderLines {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $LineItems = $Order.lineItems.edges.node
        $SpecialOrderLines = $LineItems | Where-Object {$_.TervisProperties.isSpecialOrder -eq "true"}
        $SpecialOrderLines | ForEach-Object {
            if ($_.TervisProperties.specialOrderQuantity) {
                $_.quantity = $_.TervisProperties.specialOrderQuantity
            }
        }
        return $SpecialOrderLines
    }
}

function ConvertTo-TervisShopifyOrderBTO {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $Order.EBSDocumentReference = $Order.EBSDocumentReference + "_BTO"
        return $Order
    }
}
