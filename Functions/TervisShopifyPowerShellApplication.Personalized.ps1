function Get-TervisShopifyPersonalizableItems {
    $pItems = Invoke-EBSSQL "SELECT segment1 FROM apps.mtl_system_items_b WHERE organization_id = 85 AND inventory_item_status_code = 'Active' AND segment1 LIKE '%P'"
    $pItems | ForEach-Object {
        $_.segment1.Trim("P")
    }
}

function Invoke-TervisShopifyPersonalizableItemListUpload {
    param (
        [Parameter(Mandatory)]$PackagePath,
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )
    $Branch = switch ($Environment) {
        "Delta" { "delta"; break }
        "Epsilon" { "epsilon"; break}
        "Production" { "master"; break}
    }

    Write-EventLog -LogName Shopify -Source "Personalizable Item List Upload" -EntryType Information -EventId 1 `
        -Message "Starting personalizable item upload"
    try {
        Set-Location -Path $PackagePath
        git checkout $Branch
        $PersonalizableItems = Get-TervisShopifyPersonalizableItems 
        $PersonalizableItems | ConvertTo-Json -Compress | Out-File -FilePath "./TervisPersonalizableItems.json" -Force -Encoding utf8
        $CommitMessage = "$(Get-Date -Format 'yyyyMMdd_HHmmss') - $($PersonalizableItems.Count) items"
        git commit -a -m "'$($CommitMessage)'"
        $PatchNumber = npm version patch
        if ($Environment -eq "Production") { npm publish }
        git push origin $Branch
        Write-EventLog -LogName Shopify -Source "Personalizable Item List Upload" -EntryType Information -EventId 1 `
            -Message "Personalizable item list updated to $PatchNumber`nCommit: '$CommitMessage'"
    } catch {
        Write-EventLog -LogName Shopify -Source "Personalizable Item List Upload" -EntryType Error -EventId 2 `
            -Message "Something went wrong.`nReason:`n$_`n$($_.InvocationInfo.PositionMessage)"
    }
}

function New-TervisShopifyCustomerSuppliedProperties {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$CustomAttributes
    )
    process {
        $Side1CustomerSupplied = if ($CustomAttributes.Side1IsCustomerSuppliedDecoration -eq "true") {
            "Other"
        } elseif ($CustomAttributes.Side1 -eq "true") {
            "PersGrap"
        } else {""}
        $Side2CustomerSupplied = if ($CustomAttributes.Side2IsCustomerSuppliedDecoration -eq "true") {
            "Other"
        } elseif ($CustomAttributes.Side2 -eq "true") {
            "PersGrap"
        } else {""}
        $DecorationNotes = @()
        if ($CustomAttributes.Side1CustomerSuppliedDecorationNote) {
            $DecorationNotes += "SIDE 1 ~~CUSTSUP~~$($CustomAttributes.Side1CustomerSuppliedDecorationNote)"
        }
        if ($CustomAttributes.Side2CustomerSuppliedDecorationNote) {
            $DecorationNotes += "SIDE 2 ~~CUSTSUP~~$($CustomAttributes.Side2CustomerSuppliedDecorationNote)"
        }
        $DecorationNoteString = $DecorationNotes -join " " | Invoke-TervisShopifyOracleStringEscapeQuotes
        # $DecorationNoteString = $DecorationNoteString -Replace "[^\w\s.,]","_"

        return [PSCustomObject]@{
            Side1CustomerSupplied = $Side1CustomerSupplied
            Side2CustomerSupplied = $Side2CustomerSupplied
            CustomerSuppliedDecorationNote = $DecorationNoteString
        }
    }
}

function New-TervisShopifyPersonalizedObjects {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $OrigSysDocumentRef = "'$($Order.EBSDocumentReference)P'"
        $OrderedDate = $Order.createdAt | ConvertTo-TervisShopifyOracleSqlDateString
        $CustomerPONumber = "'$($OrigSysDocumentRef.Trim("'").Split("-")[1])-$($Order.customer.displayName)'"
        $PersonalizedObject = [PSCustomObject]@{
            Header = [PSCustomObject]@{
                ORDER_SOURCE_ID = "1022"
                ORIG_SYS_DOCUMENT_REF = $OrigSysDocumentRef
                ORDERED_DATE = $OrderedDate
                ORDER_TYPE = "'DTC Sales Order'"
                SHIPPING_METHOD_CODE = "'000001_FEDEX_P_GND'" # this should be a separate function to determine ship method
                CUSTOMER_NUMBER = "'$($Order.StoreCustomerNumber)'" # Trying with store customer number from order
                CUSTOMER_PO_NUMBER = $CustomerPONumber
                BOOKED_FLAG = "'Y'"
                CREATION_DATE = "sysdate"
                LAST_UPDATE_DATE = "sysdate"
                ORIG_SYS_CUSTOMER_REF = $OrigSysDocumentRef
                ORIG_SHIP_ADDRESS_REF = $OrigSysDocumentRef
                SHIP_TO_CONTACT_REF = $OrigSysDocumentRef
                ATTRIBUTE6 = "'Y'"
                PROCESS_FLAG = "'N'"
                SOURCE_NAME = "'RMS'"
                OPERATING_UNIT_NAME = "'Tervis Operating Unit'"
                CREATED_BY_NAME = "'SHOPIFY'"
                LAST_UPDATED_BY_NAME = "'SHOPIFY'"
                CUSTOMER_REQUESTED_DATE = "sysdate"
            }
            Customer = [PSCustomObject]@{
                ORIG_SYS_DOCUMENT_REF = $OrigSysDocumentRef
                PARENT_CUSTOMER_REF = "'$($Order.StoreCustomerNumber)'" # Trying with store customer number from order
                PERSON_FIRST_NAME = "'$($Order.customer.firstName)'"
                PERSON_LAST_NAME = "'$($Order.customer.lastName)'"
                ADDRESS1 = "'$($Order.customer.defaultAddress.address1)'"
                ADDRESS2 = "'$($Order.customer.defaultAddress.address2)'"
                CITY = "'$($Order.customer.defaultAddress.city)'"
                STATE = "'$($Order.customer.defaultAddress.province)'"
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
                CUSTOMER_INFO_REF = $OrigSysDocumentRef
                IS_SHIP_TO_ADDRESS = "'Y'"
                IS_BILL_TO_ADDRESS = "'N'"
                FREIGHT_TERMS = "'Freight Collect'"
                SHIP_METHOD_CODE = "'000001_FEDEX_P_GND'"
            }
            LineItems = @()
        }

        $PersonalizationLines = $Order.lineItems.edges.node | 
            Where-Object name -Match "Personalization for" # This should be updated to look for a specific SKU or something
        
        $LineCounter = 0
        $PersonalizedObject.LineItems += foreach ($Line in $PersonalizationLines) {
            $LineCounter++
            $CustomAttributes = $Line | Convert-TervisShopifyCustomAttributesToObject
            $CustomerSuppliedProperties = $CustomAttributes | New-TervisShopifyCustomerSuppliedProperties

            [PSCustomObject]@{
                # For EBS
                ORDER_SOURCE_ID = "'1022'"
                ORIG_SYS_DOCUMENT_REF = $OrigSysDocumentRef
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
                INVENTORY_ITEM = "'$($CustomAttributes.RelatedLineItemSKU)'"
                ORDERED_QUANTITY = "$($Line.quantity)"
                UNIT_SELLING_PRICE = "0" # $Line.discountedUnitPriceSet.shopMoney.amount # Might not be relevant
                UNIT_LIST_PRICE = "0"
                
                SIDE1_FONT = "'$($CustomAttributes.Side1FontName)'"
                SIDE1_COLOR = "'$($CustomAttributes.Side1ColorName)'"
                ATTRIBUTE1 = "'$($CustomerSuppliedProperties.Side1CustomerSupplied)'" # CustomerProvided = PersGrap, Other              
                SIDE1_TEXT1 = "'$($CustomAttributes.Side1Line1 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE1_TEXT2 = "'$($CustomAttributes.Side1Line2 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE1_TEXT3 = "'$($CustomAttributes.Side1Line3 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                
                SIDE2_FONT = "'$($CustomAttributes.FontName)'"
                SIDE2_COLOR = "'$($CustomAttributes.FontColor)'"
                ATTRIBUTE7 = "'$($CustomerSuppliedProperties.Side2CustomerSupplied)'" # CustomerProvided = PersGrap, Other
                SIDE2_TEXT1 = "'$($CustomAttributes.Side2Line1 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE2_TEXT2 = "'$($CustomAttributes.Side2Line2 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"
                SIDE2_TEXT3 = "'$($CustomAttributes.Side2Line3 | Invoke-TervisShopifyOracleStringEscapeQuotes)'"

                ATTRIBUTE14 = "'$($CustomerSuppliedProperties.CustomerSuppliedDecorationNote)'"
            }
        }

        return $PersonalizedObject
    }
}

function Test-TervisShopifyIsPersonalizedOrder {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Order
    )
    process {
        $Order.EBSDocumentReference[-1] -eq "P"
    }
}

function Invoke-TervisShopifyOracleStringEscapeQuotes {
    param (
        [Parameter(ValueFromPipeline)]$String
    )
    process {
        $String -replace "'","''"
    }
}

# New method to take Order objects and convert them to an EBS query. Dynamic based
# on PSCustomObject property names, instead of manual mapping done earlier with
# Convert-TervisShopify*ToEBS* and New-EBS*Subquery
function Convert-TervisShopifyOrderObjectToEBSQuery {
    param (
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$Header,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$Customer,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$LineItems
    )
    begin {
        $Query = @"
INSERT ALL

"@
    }
    process {
        # Convert to header
        $HeaderProperties = $Header | 
            Get-Member -MemberType NoteProperty | 
            Select-Object -ExpandProperty Name
        $HeaderPropertyValues = $HeaderProperties | ForEach-Object {
            $Header.$_
        }
        $Query += "INTO xxoe_headers_iface_all ($($HeaderProperties -join ","))`n"
        $Query += "VALUES ($($HeaderPropertyValues -join ","))`n"

        # Convert to customer
        $CustomerProperties = $Customer | 
            Get-Member -MemberType NoteProperty | 
            Select-Object -ExpandProperty Name
        $CustomerValues = $CustomerProperties | ForEach-Object {
            $Customer.$_
        }
        $Query += "INTO xxoe_customer_info_iface_all ($($CustomerProperties -join ","))`n"
        $Query += "VALUES ($($CustomerValues -join ","))`n"
        # Convert to lines
        foreach ($Line in $LineItems) {
            $LineProperties = $Line | 
                Get-Member -MemberType NoteProperty | 
                Select-Object -ExpandProperty Name
            $LineValues = $LineProperties | ForEach-Object {
                $Line.$_
            }
            $Query += "INTO xxoe_lines_iface_all ($($LineProperties -join ","))`n"
            $Query += "VALUES ($($LineValues -join ","))`n"
        }
        # Convert to payment (later, after personalization/EA)
    }
    end {
        $Query += "SELECT 1 FROM DUAL`n"
        $Query
    }
}
