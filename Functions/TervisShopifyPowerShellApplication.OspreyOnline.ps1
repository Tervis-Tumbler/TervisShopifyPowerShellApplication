function New-TervisShopifyItemDescription {
    param (
        [Parameter(ValueFromPipelineByPropertyName)]$Web_Primary_Name,
        [Parameter(ValueFromPipelineByPropertyName)]$Web_Secondary_Name,
        [Parameter(ValueFromPipelineByPropertyName)]$Item_Description,
        [Parameter(ValueFromPipelineByPropertyName)]$Design_Collection
    )

    $Title = $Web_Primary_Name | ConvertTo-ShopifyFriendlyString
    $SecondaryName = $Web_Secondary_Name | ConvertTo-ShopifyFriendlyString
    $DescriptionObject = $Item_Description | ConvertFrom-TervisEBSItemDescription
    $DisplaySize = if ($DescriptionObject) { "- $($DescriptionObject.DisplaySize)" }
    $HTML = Get-TervisShopifyItemDescriptionHTML -Item_Description $Item_Description -Design_Collection $Design_Collection
    return "<p>$SecondaryName $DisplaySize</p>`n$HTML "  
} 

function Get-TervisShopifyItemDescriptionHTML {
    param (
        $Item_Description,
        $Design_Collection
    )
    $DescriptionObject = $Item_Description | ConvertFrom-TervisEBSItemDescription
    if (-not $DescriptionObject -and -not $Design_Collection) { return }
    $Collection = Get-TervisShopifySuperCollectionName -Collection $Design_Collection

    if ($Collection -eq "Accessories") {
        return @"
<div name="Accessory">
    <p>
        Accessories to complement your Tervis quick sip.
    </p>
    <p>
        <h5>Information</h5>
        <ul>
            <li>This accessory is not included in the Made for Life Guarantee.</li>
        </ul>
    </p>
</div>
"@
    }

    if ($DescriptionObject.Form -eq "SS") {
        return @"
<div name="SS">
    <p>
        This double-walled tumbler offers all the benefits of stainless steel and can be enjoyed with legendary and
        beloved exclusive Tervis designs.
    </p>
    <p>
        <h5>Information</h5>
        <ul>
            <li>Up to 24 hours cold and 8 hours hot</li>
            <li>Double-wall vacuum insulation and copper lined 18/8 stainless steel with leak resistant &amp;
                easy-close lid</li>
            <li>Do not put tumbler in dishwasher, freezer or microwave</li>
            <li>Hand wash with soap (no bleach or chlorine) &amp; water using a scratch-free sponge</li>
            <li>Tumbler designed &amp; decorated in Venice, FL. Made in China. Limited 5-year guarantee</li>
        </ul>
    </p>
</div>
"@
    }

    if ($DescriptionObject.Form -in "WB","DWT","WAV","MUG","WINE","SIP","BEER") {
        return @"
<div name="Classic">
    <p>
        Tervis insulated drinkware delivers the ultimate combination of personality and performance for pure
        drinking enjoyment.
    </p>
    <p>
        <h5>Information</h5>
        <ul>
            <li>Made in America &amp; Lifetime Guarantee</li>
            <li>This tumbler is BPA free</li>
            <li>Great for both hot &amp; cold</li>
            <li>Microwave and dishwasher safe</li>
            <li>Reduces condensation</li>
        </ul>
    </p>
</div>
"@
    }
}