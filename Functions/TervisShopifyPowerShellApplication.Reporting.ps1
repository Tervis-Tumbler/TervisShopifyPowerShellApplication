#Requires -Module TervisPasswordstatePowerShell
#Requires -Module posh-ssh

function Send-TervisShopifyReportingShopperTrakSalesData {
    $ReportContext = Get-TervisPasswordstatePassword -Guid "bbc460b6-d042-4a3a-ae03-9cc636d15de5"
    $File = Get-ChildItem -Path $ReportContext.URL -Filter SALES_*.txt | Sort-Object LastWriteTime | Select -Last 1
    $Data = Get-Content -Path $File.FullName
    $IsDate = $File.Name -match '\d+'
    if ($IsDate) {
        $FilterString = $Matches[0]
    } else {
        throw "No date string found in latest ShopperTrak file."
    }
    $FilteredData = $Data | Where-Object {$_ -like "*$FilterString*"}
    $OutboundFile = "$($File.DirectoryName)\Outbound\$($File.Name)"
    $FilteredData | Out-File -FilePath $OutboundFile -Force
    $ShopperTrakSFTPCredential = Get-PasswordstatePassword -ID 6424 -AsCredential
    $ShopperTrakSFTPURL = "data.shoppertrak.com"
    $SFTPSession = New-SFTPSession -ComputerName $ShopperTrakSFTPURL -Credential $ShopperTrakSFTPCredential -AcceptKey
    Set-SFTPFile -SFTPSession $SFTPSession -LocalFile $OutboundFile -Verbose -RemotePath "/"
}