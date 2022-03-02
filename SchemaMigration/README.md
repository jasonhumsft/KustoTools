# Schema Migration Tool
This tool is created to export the definition of databases in a source Azure Data Explorer cluster, and import into a target cluster.

# What will be done for export
1. The export will include policies, tables, functions, materialized views, ingestion mappings, etc. 
2. Everything included will be exported as JSON files in a local folder.

# What will be done for import
1. Create databases
2. Import everything exported

# Dependent Python modules (and tested versions)
- azure-cli-core==2.33.1
- azure-identity==1.7.1
- azure-kusto-data==3.0.1
- azure-kusto-ingest==3.0.1
- azure-mgmt-kusto==2.1.0
- msrestazure==0.6.4
- numpy==1.22.2

# Usage
## Export
python KustoSchemaMigrateTool.py export -z -c https://sourceadxcluster.westus2.kusto.windows.net -d my-default-db-for-connection
## Import
python KustoSchemaMigrateTool.py import -z -c https://sourceadxcluster.westus2.kusto.windows.net -e https://targetadxcluster.westus2.kusto.windows.net -i target-subscription-id -r targer-resource-group -l 'TheLocationForTargetCluster'
## Migrate (Export & Import)
python KustoSchemaMigrateTool.py migrate -z -c https://sourceadxcluster.westus2.kusto.windows.net -d my-default-db-for-connection -e https://targetadxcluster.westus2.kusto.windows.net -i target-subscription-id -r target-resource-group -l 'TheLocationForTargetCluster'
## Builtin Help
python KustoSchemaMigrateTool.py --help
