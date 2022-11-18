function Get-TervisShopifyEndlessAisleItems {
    $Query = "SELECT intf.item_number FROM xxtrvs.xxtrvs_store_item_price_intf intf,apps.mtl_system_items_b msi WHERE intf.internal_order_enabled_flag = 'Y' AND intf.item_status = 'Active' AND intf.item_number NOT LIKE '%P' AND intf.item_id = msi.inventory_item_id AND msi.organization_id = 143"
    $EndlessAisleItems = Invoke-EBSSQL $Query | Select-Object -ExpandProperty item_number
    return $EndlessAisleItems
}

function Invoke-TervisShopifyEndlessAisleItemListUpload {
    param (
        [Parameter(Mandatory)][ValidateSet("Delta","Epsilon","Production")]$Environment
    )

    Write-EventLog -LogName Shopify -Source "Shopify Azure Blob" -EntryType Information -EventId 1 `
        -Message "Starting EndlessAisle item upload"
    try {
        $EndlessAisleItems = Get-TervisShopifyEndlessAisleItems
        $JsonString = $EndlessAisleItems | ConvertTo-Json -Compress
        $Url = Set-TervisShopifyAzureBlob -BlobName "EndlessAisle_$Environment" -Content $JsonString

        Write-EventLog -LogName Shopify -Source "Shopify Azure Blob" -EntryType Information -EventId 1 `
            -Message "EndlessAisle_$Environment successfully uploaded to Azure Blob Storage at:`n$Url"
    } catch {
        Write-EventLog -LogName Shopify -Source "Shopify Azure Blob" -EntryType Error -EventId 2 `
            -Message "Something went wrong.`nReason:`n$_"
    }
}