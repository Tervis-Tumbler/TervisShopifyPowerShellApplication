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
    Get-ShopifyOrders -ShopName $ShopName -QueryString "tag:PTest4" | Get-TervisShopifyOrdersForImport -ShopName $ShopName
}

function Get-TervisShopifyEndlessAisleTestOrder {
    param (
        [Parameter(Mandatory)]$ShopName
    )
    Get-ShopifyOrders -ShopName $ShopName -QueryString "tag:ShipTest" | Get-TervisShopifyOrdersForImport -ShopName $ShopName
}

function Get-TervisShopifyEBSRecord {
  param (
    [Parameter(Mandatory)]$OrigSysDocumentRef
  )
  
  $Tables =   "xxoe_headers_iface_all",
              "xxoe_customer_info_iface_all",
              "xxoe_lines_iface_all",
              "xxoe_payments_iface_all"

  foreach ($Table in $Tables) {
    Write-Warning "$Table"
    Invoke-EBSSQL -SQLCommand "SELECT * FROM $Table WHERE orig_sys_document_ref = '$OrigSysDocumentRef'"
  }
}