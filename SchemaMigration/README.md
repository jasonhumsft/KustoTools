# Schema Migration Tool
This tool is created to export the definition of databases from a source Azure Data Explorer cluster, and import into a target cluster.

## What will be done for export
1. The export will include policies, tables, functions, materialized views, ingestion mappings, etc. 
2. Everything included will be exported as JSON files in a local folder.

## What will be done for import
1. Create databases
2. Import everything exported

## Required permissions
### For export
Azure Data Explor role like "All Databases viewer" or "All Databases admin" is needed on the source cluster for the used user/app principal. Such role is assigned at Azure Portal -> Azure Data Explorer Clusters -> Select the cluster -> "Permissions" blade under "Security + networking".

### For import
IAM role like "Contributor" or "Owner" on the target cluster resource is needed for the used user/app principal. Such role is assigned at Azure Portal -> Azure Data Explorer Clusters -> Select the cluster -> "Access control (IAM)" blade.

Azure Data Explor role "All Databases admin" on the target cluster is needed for the used user/app principal. Such role is assigned at Azure Portal -> Azure Data Explorer Clusters -> Select the cluster -> "Permissions" blade under "Security + networking".

## Authentication
"AZ CLI" authentication and "AAD application" authentication is supported. Refer to https://github.com/Azure/azure-kusto-python to understand the difference.
- To use "AZ CLI" authentication, specify the option "-z"; 
- To use "AAD application" authentication, create an AAD application ahead (refer to: https://docs.microsoft.com/en-us/azure/data-explorer/provision-azure-ad-app), and provide values for options "-a", "-s" and "-t".

## Usage
### Export
python KustoSchemaMigrateTool.py export -z -c https://sourceadxcluster.westus2.kusto.windows.net
### Import
python KustoSchemaMigrateTool.py import -z -c https://sourceadxcluster.westus2.kusto.windows.net -e https://targetadxcluster.westus2.kusto.windows.net -i target-subscription-id -r targer-resource-group -l 'TheLocationForTargetCluster'
### Migrate (Export & Import)
python KustoSchemaMigrateTool.py migrate -z -c https://sourceadxcluster.westus2.kusto.windows.net -e https://targetadxcluster.westus2.kusto.windows.net -i target-subscription-id -r target-resource-group -l 'TheLocationForTargetCluster'
### Builtin Help
python KustoSchemaMigrateTool.py --help

## Dependent Python modules (and tested versions)
- azure-cli-core==2.33.1
- azure-identity==1.7.1
- azure-kusto-data==3.0.1
- azure-kusto-ingest==3.0.1
- azure-mgmt-kusto==2.1.0
- msrestazure==0.6.4
- numpy==1.22.2