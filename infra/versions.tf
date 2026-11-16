terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}

  # Storage access keys are disabled on the lake, so use Entra ID for storage calls.
  storage_use_azuread = true

  # Required by azurerm 4.x. Set it in terraform.tfvars or with ARM_SUBSCRIPTION_ID.
  subscription_id = var.subscription_id
}
