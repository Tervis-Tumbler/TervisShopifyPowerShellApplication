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
