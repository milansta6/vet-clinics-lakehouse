output "resource_group_name" {
  description = "Name of the resource group."
  value       = azurerm_resource_group.this.name
}

output "storage_account_name" {
  description = "Name of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.lake.name
}

output "lake_dfs_endpoint" {
  description = "ADLS Gen2 endpoint, used in abfss:// paths and Unity Catalog external locations."
  value       = azurerm_storage_account.lake.primary_dfs_endpoint
}

output "landing_abfss_url" {
  description = "abfss URL of the landing container."
  value       = "abfss://landing@${azurerm_storage_account.lake.name}.dfs.core.windows.net/"
}

output "databricks_workspace_url" {
  description = "URL of the Databricks workspace."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "databricks_access_connector_id" {
  description = "Resource id of the access connector (needed to create the Unity Catalog storage credential)."
  value       = azurerm_databricks_access_connector.unity.id
}
