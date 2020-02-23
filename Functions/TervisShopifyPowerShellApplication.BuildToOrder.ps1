function New-TervisShopifyBuildToOrderObject {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $OrderObject = $Order| New-TervisShopifyOrderObjectBase
        $OrderObject | Add-TervisShopifyBuildToOrderHeaderProperties -Order $Order
        $OrderObject.Customer = $Order | New-TervisShopifyBuildToOrderCustomerInfo
        # Next up, create Personalization and Special Order lines
        # Add TervisPropterties to all llne items 
        $Order.lineItems.edges.node | Add-TervisShopifyLineItemProperties
        # Personalization:
        $PersonalizationLineItems = $Order | 
            Select-TervisShopifyOrderPersonalizationLines | 
            Add-TervisShopifyOrderPersonalizationSKU 
        # Special Order
        $SpecialOrderLineItems = $Order | Select-TervisShopifyOrderSpecialOrderLines
        Write-Warning "Pers"
        $PersonalizationLineItems
        Write-Warning "SO"
        $SpecialOrderLineItems
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
            # # SHIP_FROM_ORG = "ORG" not in here. Maybe remove from head?
        }
        
        foreach ($Property in $PropertiesToAdd.Keys) {
            $OrderObject.Header | Add-Member -MemberType NoteProperty -Name $Property -Value $PropertiesToAdd[$Property]
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
