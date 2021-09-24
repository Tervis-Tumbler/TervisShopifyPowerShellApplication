# Requires -Module TervisPasswordstatePowerShell
# Requires -Module posh-ssh

function Send-TervisShopifyReportingShopperTrakSalesData {
    $ReportContext = Get-TervisPasswordstatePassword -Guid "bbc460b6-d042-4a3a-ae03-9cc636d15de5"
    $ShopperTrakSFTPCredential = Get-PasswordstatePassword -ID 6429 -AsCredential
    $ShopperTrakSFTPURL = "data.shoppertrak.com"
    $Files = Get-ChildItem -Path "$($ReportContext.URL)\Inbound" -Filter SALES_*.txt
    foreach ($File in $Files) {
        $Data = $null
        $IsDate = $null
        $Data = Get-Content -Path $File.FullName
        $IsDate = $File.Name -match '\d+'
        if ($IsDate) {
            $FilterString = $Matches[0]
        } else {
            throw "No date string found in latest ShopperTrak file."
        }
        $FilteredData = $Data | Where-Object {$_ -like "*$FilterString*"}
        $SubstitutedData = $FilteredData | Set-TervisShopifyReportingStoreIDs
        $OutboundFile = "$($ReportContext.URL)\Outbound\$($File.Name)"
        $SubstitutedData | Out-File -FilePath $OutboundFile -Force -Encoding ascii
        $SFTPSession = New-SFTPSession -ComputerName $ShopperTrakSFTPURL -Credential $ShopperTrakSFTPCredential -AcceptKey
        Set-SFTPFile -SFTPSession $SFTPSession -LocalFile $OutboundFile -Verbose -RemotePath "/"
        Move-Item -Path $File.FullName -Destination "$($ReportContext.URL)\ArchivedInbound"
    }
}

function Set-TervisShopifyReportingStoreIDs {
    param (
        [Parameter(ValueFromPipeline)]$ReportContent
    )
    begin {
        $SubstitutionTable = Import-Csv -Path "\\tervis.prv\applications\Shopify\ShopperTrak\Config\Paylocity_Shopify_StoreIDs.csv"
        $Result = ""
    }
    process {
        $SplitContent = $ReportContent -split ","
        foreach ($Substitution in $SubstitutionTable) {
            if ($SplitContent[0] -eq $Substitution.SalesVendorID) {
                $SplitContent[0] = $Substitution.StoreID
            }
        }
        $Result += "$($SplitContent -join ",")`n"
    }
    end {
        $Result
    }
}