variable "subscription_id" {
  description = "Azure subscription id. Leave null to use the ARM_SUBSCRIPTION_ID environment variable."
  type        = string
  default     = null
}

variable "project" {
  description = "Short project name used in resource names (lower-case letters and digits only)."
  type        = string
  default     = "vetlake"

  validation {
    condition     = can(regex("^[a-z0-9]{3,10}$", var.project))
    error_message = "project must be 3-10 lower-case letters or digits."
  }
}

variable "environment" {
  description = "Environment name, for example dev, test or prod."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be one of: dev, test, prod."
  }
}

variable "location" {
  description = "Azure region. West Europe keeps the data inside the EU."
  type        = string
  default     = "westeurope"
}

variable "storage_replication_type" {
  description = "Storage redundancy. LRS is the cheapest; use ZRS or GRS for production."
  type        = string
  default     = "LRS"
}

variable "lake_containers" {
  description = "Containers (file systems) to create in the data lake."
  type        = set(string)
  default     = ["landing", "bronze", "silver", "gold"]
}

variable "tags" {
  description = "Extra tags added to every resource."
  type        = map(string)
  default     = {}
}
