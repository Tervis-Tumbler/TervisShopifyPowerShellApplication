function Get-TervisShopifyEnvironmentShopName {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )

    switch ($Environment) {
        "Delta" {"DLT-TervisStore"; break}
        "Epsilon" {"SIT-TervisStore"; break}
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
            if ($Attribute.key) {
                $Value = $Attribute.value | Invoke-TervisShopifyOracleStringEscapeQuotes
                $Object | Add-Member -MemberType NoteProperty -Name $Attribute.key -Value $Value -Force
            }
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
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$LineItems,
        [Parameter(Mandatory,ValueFromPipelineByPropertyName)]$Payments
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
        if ($CustomerProperties) {
            $Query += "INTO xxoe_customer_info_iface_all ($($CustomerProperties -join ","))`n"
            $Query += "VALUES ($($CustomerValues -join ","))`n"
        }

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
        foreach ($Payment in $Payments) {
            $PaymentProperties = $Payment | 
                Get-Member -MemberType NoteProperty | 
                Select-Object -ExpandProperty Name
            $PaymentValues = $PaymentProperties | ForEach-Object {
                $Payment.$_
            }
            if ($Payment) {
                $Query += "INTO xxoe_payments_iface_all ($($PaymentProperties -join ","))`n"
                $Query += "VALUES ($($PaymentValues -join ","))`n"
            }
        }
    }
    end {
        $Query += "SELECT 1 FROM DUAL`n"
        return $Query
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

function Split-ArrayIntoArrays {
    param (
        [Parameter(Mandatory,ValueFromPipeline)][array]$InputObject,
        [Parameter(Mandatory)][int]$NumberOfArrays
    )
    $ParentArray = @()
    $SubarrayLength = [System.Math]::Ceiling( $InputObject.Count / $NumberOfArrays )
    $Cursor = 0
    for ($i = 0; $i -lt $NumberOfArrays; $i++) {
        [array]$Subarray = $InputObject | Select-Object -First $SubarrayLength -Skip $Cursor
        $Cursor += $SubarrayLength
        $ParentArray += ,($Subarray)
    }
    return $ParentArray
}

function ConvertFrom-TervisEBSItemDescription {
    param (
        [Parameter(Mandatory,ValueFromPipeline)]$ItemDescription
    )
    process {
        $SplitDescription = $ItemDescription -split "\."
        if ($SplitDescription.Count -NE 8) {
            # throw "$ItemDescription does not define all item attributes"
            return
        }
        $DisplaySize = "$($SplitDescription[4]) $($SplitDescription[5].ToLower())"
        # $DisplayCupColor = "Cup: $($SplitDescription[2] | ConvertFrom-TervisColorCode)"
        # $DisplayLidColor = "Lid: $($SplitDescription[7] | ConvertFrom-TervisColorCode)"
        # $DisplayUOM
    
        return [PSCustomObject]@{
            Original = $ItemDescription
            Name = $SplitDescription[0]
            Form = $SplitDescription[1]
            CupColor = $SplitDescription[2]
            Decoration = $SplitDescription[3]
            SizeValue = $SplitDescription[4]
            SizeUnit = $SplitDescription[5]
            UnitOfMeasure = $SplitDescription[6]
            LidColor = $SplitDescription[7]
            DisplaySize = $DisplaySize
        }
    }
}

function Set-TervisColorCodeDefinition {
    $ColorCodeURL = "https://unpkg.com/@tervis/tervisproductmetadata/ColorCodeToMarketingNameMapping.json"
    $Script:ColorCode = Invoke-RestMethod -Uri $ColorCodeURL -Method Get
}

function ConvertFrom-TervisColorCode {
    param (
        [Parameter(Mandatory)]$ColorCode
    )
    if ($ColorCodeURL -EQ "NA") {return "N/A"}
    if (-not $Script:ColorCode) {Set-TervisColorCodeDefinition}
    $Result = $Script:ColorCode | Where-Object ColorCode -EQ $ColorCode | Select-Object -ExpandProperty MarketingName
    if ($Result) {
        return $Result
    } else {
        throw "$ColorCode is not a valid color code."
    }
}

function ConvertTo-ShopifyFriendlyString {
    param (
        [Parameter(ValueFromPipeline)][string]$String
    )
    process {
        if (-not $String) {return}
        $CharArray = $String.ToCharArray()
        $StringArray = $CharArray | ForEach-Object {
            $CharValue = [char]::ConvertToUtf32($_.ToString(), 0)
            if ($CharValue -gt 127) {
                return "\u{0:x4}" -f $CharValue
            } else {
                return $_.ToString()
            }
        }
        return $StringArray -join ""
    }
}


function Get-TervisShopifyAzureBlobCredential {
    $Guid = "d1f46ea2-45d0-4047-acee-5fa80fdcfa87"
    if ( -not $Script:TervisShopifyAzureBlobCredential) {
        $Script:TervisShopifyAzureBlobCredential = Get-TervisPasswordstatePassword -Guid $Guid -AsCredential
    }
    return $Script:TervisShopifyAzureBlobCredential
}

function Set-TervisShopifyAzureBlob {
    param (
        [Parameter(Mandatory)]$BlobName,
        [Parameter(Mandatory)]$Content
    )
     
    $Credential = Get-TervisShopifyAzureBlobCredential

    $method = "PUT"
    $headerDate = '2019-07-07'
    $headers = @{"x-ms-version"="$headerDate"}
    $StorageAccountName = $Credential.UserName
    $StorageContainerName = "tervisshopify"
    $StorageAccountKey = $Credential.GetNetworkCredential().Password
    $Url = "https://$StorageAccountName.blob.core.windows.net/$StorageContainerName/$BlobName"
    $body = $Content
    $xmsdate = (Get-Date ([System.DateTimeOffset]::UtcNow).DateTime -Format r).ToString()
    $bytes = ([System.Text.Encoding]::UTF8.GetBytes($body))
    $contentLength = $bytes.length

    $headers.Add("x-ms-date",$xmsdate)
    $headers.Add("Content-Length","$contentLength")
    $headers.Add("x-ms-blob-type","BlockBlob")

    $signatureString = "$method`n`n`n$contentLength`n`n`n`n`n`n`n`n`n"
    #Add CanonicalizedHeaders
    $signatureString += "x-ms-blob-type:" + $headers["x-ms-blob-type"] + "`n"
    $signatureString += "x-ms-date:" + $headers["x-ms-date"] + "`n"
    $signatureString += "x-ms-version:" + $headers["x-ms-version"] + "`n"
    #Add CanonicalizedResource
    $uri = New-Object System.Uri -ArgumentList $url
    $signatureString += "/" + $StorageAccountName + $uri.AbsolutePath

    $dataToMac = [System.Text.Encoding]::UTF8.GetBytes($signatureString)

    $accountKeyBytes = [System.Convert]::FromBase64String($StorageAccountKey)

    $hmac = new-object System.Security.Cryptography.HMACSHA256((,$accountKeyBytes))
    $signature = [System.Convert]::ToBase64String($hmac.ComputeHash($dataToMac))

    $headers.Add("Authorization", "SharedKey " + $StorageAccountName + ":" + $signature);

    try {
        Invoke-RestMethod -Uri $Url -Method $method -headers $headers -Body $body
        Write-EventLog -LogName Shopify -Source "Shopify Azure Blob" -EntryType Information -EventId 1 `
            -Message "$BlobName successfully uploaded to Azure Blob Storage at:`n$Url"
    } catch {
        Write-EventLog -LogName Shopify -Source "Shopify Azure Blob" -EntryType Error -EventId 2 `
            -Message "$BlobName could not be uploaded to Azure Blob Storage. Reason:`n$_`n$($_.InvocationInfo.PositionMessage)"
    }
}