function Invoke-ShopifyGraphQLTest{
    param (
        [Parameter(Mandatory)]$ShopName
    )
    $Body = @"
    {
        shop {
          products(first: 250) {
            edges {
              node {
                id
                handle
              }
            }
            pageInfo {
              hasNextPage
            }
          }
        }
      }
"@

    Invoke-ShopifyAPIFunction -ShopName $ShopName -Body $Body
}

function Get-TervisShopifyPersonalizedTestOrder {
    param (
        [Parameter(Mandatory)]$ShopName
    )
    Get-ShopifyOrders -ShopName $ShopName -QueryString "tag:PTest3" | Get-TervisShopifyOrdersForImport -ShopName $ShopName
}
