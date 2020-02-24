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
        # NEXT STEPS:
        # Create "fork in road" for build to order from main order import process
        # Test regular order import process
        # Test with combined special order/ personalized tumblers
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

        $OrigSysDocumentRef = $OrderObject.Header.ORIG_SYS_DOCUMENT_REF
        $CustomerPONumber = "'$($OrigSysDocumentRef.Trim("'").Split("-")[1])-$($Order.customer.displayName)'"

        $PropertiesToAdd = @{
            SHIPPING_METHOD_CODE = "'$($ShipMethodCode)'"
            CUSTOMER_PO_NUMBER = $CustomerPONumber
            ORIG_SYS_CUSTOMER_REF = $OrigSysDocumentRef
            ORIG_SHIP_ADDRESS_REF = $OrigSysDocumentRef
            SHIP_TO_CONTACT_REF = $OrigSysDocumentRef
            ATTRIBUTE6 = "'$($FreeFreight)'" 
            CUSTOMER_REQUESTED_DATE = "sysdate"
            ORDER_TYPE = "'DTC Sales Order'"
            # # SHIP_FROM_ORG = "ORG" not in here. Maybe remove from head?
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
        [PSCustomObject]@{
            ORIG_SYS_DOCUMENT_REF = "'$($Order.EBSDocumentReference)'"
            PARENT_CUSTOMER_REF = "'$($Order.StoreCustomerNumber)'" # Trying with store customer number from order
            PERSON_FIRST_NAME = "'$($Order.customer.firstName)'"
            PERSON_LAST_NAME = "'$($Order.customer.lastName)'"
            ADDRESS1 = "'$($Order.customer.defaultAddress.address1)'"
            ADDRESS2 = "'$($Order.customer.defaultAddress.address2)'"
            CITY = "'$($Order.customer.defaultAddress.city)'"
            STATE = "'$($Order.customer.defaultAddress.province)'" # This still returns full state name. Check GraphQL query.
            POSTAL_CODE = "'$($Order.customer.defaultAddress.zip)'"
            COUNTRY = "'$($Order.customer.defaultAddress.countryCodeV2)'"
            PROCESS_FLAG = "'N'"
            SOURCE_NAME = "'RMS'"
            OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
            CREATED_BY_NAME = "'SHOPIFY'"
            LAST_UPDATED_BY_NAME = "'SHOPIFY'"
            CREATION_DATE = "sysdate"
            # Below only applies to CUSTOMER_TYPE "ORGANIZATION" 
            PARTY_ID = "360580"
            CUSTOMER_TYPE = "'ORGANIZATION'"
            ORGANIZATION_NAME = "'$($Order.customer.displayName)'"
            CUSTOMER_INFO_TYPE_CODE = "'ADDRESS'"
            CUSTOMER_INFO_REF = "'$($Order.EBSDocumentReference)'"
            IS_SHIP_TO_ADDRESS = "'Y'"
            IS_BILL_TO_ADDRESS = "'N'"
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
                $LineNote = $CustomerSuppliedProperties.CustomerSuppliedDecorationNote
                $InventoryItem = $Line.TervisProperties.RelatedLineItemSKU
            }

            [PSCustomObject]@{
                # For EBS
                ORDER_SOURCE_ID = "'1022'"
                ORIG_SYS_DOCUMENT_REF = "'$($Order.EBSDocumentReference)'"
                ORIG_SYS_LINE_REF = "'$($LineCounter)'"
                LINE_TYPE = "'Tervis Bill Only with Inv Line'"
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
                SIDE1_TEXT1 = "'$($Line.TervisProperties.Side1Line1 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE1_TEXT2 = "'$($Line.TervisProperties.Side1Line2 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE1_TEXT3 = "'$($Line.TervisProperties.Side1Line3 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                
                SIDE2_FONT = "'$($Line.TervisProperties.FontName)'"
                SIDE2_COLOR = "'$($Line.TervisProperties.FontColor)'"
                ATTRIBUTE7 = "'$($CustomerSuppliedProperties.Side2CustomerSupplied)'" # CustomerProvided = PersGrap, Other
                SIDE2_TEXT1 = "'$($Line.TervisProperties.Side2Line1 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE2_TEXT2 = "'$($Line.TervisProperties.Side2Line2 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE2_TEXT3 = "'$($Line.TervisProperties.Side2Line3 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"

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
        return $LineItems | Where-Object {$_.TervisProperties.isSpecialOrder -eq "true"}
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
