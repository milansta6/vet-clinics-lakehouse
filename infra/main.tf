# EXAMPLE infrastructure-as-code. It has never been applied to a real subscription.
# It shows the minimum Azure setup for this lakehouse:
#   resource group -> ADLS Gen2 storage account -> Databricks workspace
#   + an access connector (managed identity) that lets Unity Catalog reach the storage.

locals {
  name_suffix = "${var.project}-${var.environment}"

  tags = merge(
    {
      project     = var.project
      environment = var.environment
      managed_by  = "terraform"
    },
    var.tags,
  )
}

# Storage account names must be globally unique: add a short random suffix.
resource "random_string" "storage_suffix" {
  length  = 6
  upper   = false
  special = false
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name_suffix}"
  location = var.location
  tags     = local.tags
}

# --------------------------------------------------------------------------- #
# Data lake: ADLS Gen2 = storage account with hierarchical namespace enabled
# --------------------------------------------------------------------------- #
resource "azurerm_storage_account" "lake" {
  name                     = "st${var.project}${var.environment}${random_string.storage_suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false # identity-based access only

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }

  tags = local.tags
}

resource "azurerm_storage_container" "lake" {
  for_each = var.lake_containers

  name                  = each.value
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# Cost control: raw landing files are rarely read again after ingestion.
resource "azurerm_storage_management_policy" "lake" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "landing-to-cool-then-delete"
    enabled = true

    filters {
      prefix_match = ["landing/"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
        delete_after_days_since_modification_greater_than       = 365
      }
    }
  }
}

# --------------------------------------------------------------------------- #
# Databricks workspace (Premium is required for Unity Catalog)
# --------------------------------------------------------------------------- #
resource "azurerm_databricks_workspace" "this" {
  name                        = "dbw-${local.name_suffix}"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "premium"
  managed_resource_group_name = "rg-${local.name_suffix}-databricks-managed"
  tags                        = local.tags
}

# Managed identity used by Unity Catalog (storage credential) to access the lake.
resource "azurerm_databricks_access_connector" "unity" {
  name                = "dbac-${local.name_suffix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

resource "azurerm_role_assignment" "unity_lake_access" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity.identity[0].principal_id
}
