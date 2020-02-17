function Get-TervisShopifyEnvironmentShopName {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )

    switch ($Environment) {
        "Delta" {"DLT-TervisStore"; break}
        "Epsilon" {"DLT-TervisStore"; break}
        "Production" {"tervisstore"; break}
        default {throw "Environment not recognized"}
    }
}

function Invoke-EBSSubqueryInsert {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Subquery,
        [switch]$ShowQuery
    )
    begin {
        $FinalQuery = "INSERT ALL"
    }
    process {
        $FinalQuery += "`n$Subquery"
    }
    end {
        $FinalQuery += "`nSELECT 1 FROM DUAL"
        if ($ShowQuery) {
            return $FinalQuery
        } else {
            Invoke-EBSSQL -SQLCommand $FinalQuery
        }
    }
}

function ConvertTo-TervisShopifyOracleSqlDateString {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$DateTime
    )
    process {
        $DateTimeString = (Get-Date $DateTime -Format "yyyyMMddHHmmss")
        return "TO_DATE('$DateTimeString', 'YYYYMMDDHH24MISS')"
    }
}


function Convert-TervisShopifyCustomAttributesToObject {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$Node
    )
    process {
        $Object = [PSCustomObject]::new()
        foreach ($Attribute in $Node.customAttributes) {
            $Object | Add-Member -MemberType NoteProperty -Name $Attribute.key -Value $Attribute.value -Force
        }
        return $Object
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
