from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
import json
import os
import argparse
import logging

log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)


class KustoExportUtility:

    mapping_file_extension = ".mapping.json"
    schema_file_extension = ".schema.json"

    query_all_dbs = ".show databases | where DatabaseAccessMode =='ReadWrite' | project DatabaseName"
    query_db_definition = ".show database schema as csl script "
    query_ingestion_mapping = ".show database ingestion mappings"

    def __init__(self, cluster: str, default_database: str = "NetDefaultDB", az_cli_auth: bool = True, aad_client_id: str = None, aad_client_secret: str = None, aad_tenant_id: str = None) -> None:
        if az_cli_auth:
            self._kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(
                cluster)

        else:
            self._kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                cluster, aad_client_id, aad_client_secret, aad_tenant_id)
        self._client = KustoClient(self._kcsb)
        self._cluster = cluster
        self._database = default_database
        self._cluster_name = cluster.split(".")[0].removeprefix("https://")
        self._folder = self._cluster_name+os.sep

    def _create_export_folder(self) -> None:
        if not os.path.isdir(self._cluster_name):
            logging.info("Creating export folder "+self._cluster_name)
            os.mkdir(self._cluster_name)

    def _get_all_dbs(self) -> None:
        logging.info("Listing all readwrite databases")
        response = self._client.execute(
            self._database, KustoExportUtility.query_all_dbs)
        self._all_dbs = []
        for row in response.primary_results[0]:
            self._all_dbs.append(row["DatabaseName"])

    def _get_db_definition(self, database_name: str) -> None:
        logging.info("Getting definition for database "+database_name)
        db_def = []
        response = self._client.execute(
            database_name, KustoExportUtility.query_db_definition)
        for row in response.primary_results[0]:
            db_def.append(row[0])
        with open(self._folder+database_name+KustoExportUtility.schema_file_extension, 'w') as f:
            json.dump(db_def, f, indent=4)
            f.close()

    def _get_ingestion_mappings(self, database_name: str) -> None:
        logging.info("Getting ingestion mappings for database "+database_name)
        mappings_def = []
        response = self._client.execute(
            database_name, KustoExportUtility.query_ingestion_mapping)
        for row in response.primary_results[0]:
            map_def = ".create table ['"+row["Table"]+"'] ingestion "+(str(row["Kind"]).lower(
            ))+" mapping '"+row["Name"]+"'"+" '"+str(row["Mapping"]).replace("'", "\\'")+"'"
            mappings_def.append(map_def)
        with open(self._folder+database_name+KustoExportUtility.mapping_file_extension, 'w') as f:
            json.dump(mappings_def, f, indent=4)
            f.close()

    def start_export(self) -> None:
        self._create_export_folder()
        self._get_all_dbs()
        for db in self._all_dbs:
            self._get_db_definition(db)
            self._get_ingestion_mappings(db)


class KustoImportUtility:
    from datetime import timedelta

    soft_delete_period = timedelta(days=365)
    hot_cache_period = timedelta(days=31)

    mapping_file_extension = ".mapping.json"
    schema_file_extension = ".schema.json"

    def __init__(self, source_cluster: str, target_cluster: str, target_subscription_id: str, target_location, target_resource_group, az_cli_auth: bool = True, aad_client_id: str = None,
                 aad_client_secret: str = None, aad_tenant_id: str = None) -> None:
        if az_cli_auth:
            from azure.identity import AzureCliCredential
            self._credentials = AzureCliCredential()
            self._kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(
                target_cluster)
        else:
            from azure.identity import ClientSecretCredential
            self._credentials = ClientSecretCredential(
                client_id=aad_client_id, client_secret=aad_client_secret, tenant_id=aad_tenant_id)
            self._kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                target_cluster, aad_client_id, aad_client_secret, aad_tenant_id)

        from azure.mgmt.kusto import KustoManagementClient
        from azure.mgmt.kusto.models import ReadWriteDatabase

        self._kusto_management_client = KustoManagementClient(
            self._credentials, target_subscription_id)
        self._client = KustoClient(self._kcsb)

        self._database_operations = self._kusto_management_client.databases
        self._database_template = ReadWriteDatabase(location=target_location,
                                                    soft_delete_period=KustoImportUtility.soft_delete_period,
                                                    hot_cache_period=KustoImportUtility.hot_cache_period)

        self._resource_group = target_resource_group
        self._source_cluster_name = source_cluster.split(
            ".")[0].removeprefix("https://")
        self._target_cluster_name = target_cluster.split(
            ".")[0].removeprefix("https://")
        self._folder = self._source_cluster_name+os.sep
        self._schema_files = set()
        self._mapping_files = set()
        self._all_dbs = set()

    def _find_export_files(self) -> None:
        logging.info("Searching export files for cluster " +
                     self._source_cluster_name)
        if not os.path.isdir(self._source_cluster_name):
            logging.error(
                "No folder for source cluster found, please run export process first")
            exit(127)

        for file in os.listdir(self._source_cluster_name):
            if file.endswith(KustoImportUtility.schema_file_extension):
                self._schema_files.add(file)
            elif file.endswith(KustoImportUtility.mapping_file_extension):
                self._mapping_files.add(file)

    def _get_all_dbs(self) -> None:
        logging.info("Listing all databases from schema files")
        for f in self._schema_files:
            self._all_dbs.add(f.split(".")[0])

    def _create_database(self, database_name: str) -> None:
        logging.info("Creating database "+database_name+" on target cluster")
        poller = self._database_operations.begin_create_or_update(
            resource_group_name=self._resource_group, cluster_name=self._target_cluster_name, database_name=database_name, parameters=self._database_template)
        poller.wait()

    def _import_schema(self, database_name: str) -> None:
        logging.info("Importing schema into database " +
                     database_name+" on target cluster")
        with open(self._folder+database_name+KustoImportUtility.schema_file_extension) as schema_file:
            schema_scripts = json.load(schema_file)
            schema_file.close()
            failed_scripts = list()
            # execute script to apply schema
            for script in schema_scripts:
                try:
                    response = self._client.execute(database_name, script)
                except:
                    failed_scripts.append(script)

            for script in failed_scripts:
                try:
                    response = self._client.execute(database_name, script)
                except Exception as e:
                    logging.warning("Failed to execute script: "+script)
                    logging.warning("Error Message is: "+str(e))

    def _import_mappings(self, database_name: str) -> None:
        logging.info("Importing ingestion mappings into database " +
                     database_name+" on target cluster")
        mapping = database_name+KustoImportUtility.mapping_file_extension
        if mapping in self._mapping_files:
            with open(self._folder+mapping) as mapping_file:
                mapping_scripts = json.load(mapping_file)
                mapping_file.close()
                for script in mapping_scripts:
                    try:
                        response = self._client.execute(database_name, script)
                    except Exception as e:
                        logging.warning("Failed to execute script: "+script)
                        logging.warning("Error Message is: "+str(e))

    def start_import(self) -> None:
        self._find_export_files()
        self._get_all_dbs()

        for db in self._all_dbs:
            self._create_database(db)
            self._import_schema(db)
            self._import_mappings(db)


def args_parser() -> argparse.ArgumentParser:
    action_help = """
        export:     Export all databases schema from a cluster
        import:     Import all databases schema into a cluster (must run export ahead to get all definition files)
        migrate:    Export all databases schema from a source cluster, and import into a target schema
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('action', choices=[
                        'export', 'import', 'migrate'], help=action_help)
    parser.add_argument('-z', '--azure_cli_authentication',
                        action='store_true', help="To use AZ CLI authentication")
    parser.add_argument('-d', '--connection_db', type=str, default="NetDefaultDB",
                        help="The default database to connect during export or migration (default=NetDefaultDB)")
    parser.add_argument('-a', '--aad_app_id', type=str,
                        help="AAD app id (mandatory if you don't use AZ CLI authentication)")
    parser.add_argument('-s', '--aad_app_secret', type=str,
                        help="AAD app secret (mandatory if you don't use AZ CLI authentication)")
    parser.add_argument('-t', '--aad_tenant_id', type=str,
                        help="AAD tenant id (mandatory if you don't use AZ CLI authentication)")
    parser.add_argument('-c', '--cluster', type=str,
                        help="Source cluster URI (mandatory for export or import or migrate)")
    parser.add_argument('-e', '--target_cluster', type=str,
                        help="Target cluster URI (mandatory for import or migrate)")
    parser.add_argument('-i', '--subscription_id', type=str,
                        help="Target cluster subscription id (mandatory for import or migrate)")
    parser.add_argument('-r', '--resource_group', type=str,
                        help="Target cluster resource group name (mandatory for import or migrate)")
    parser.add_argument('-l', '--location', type=str,
                        help="Target cluster location (mandatory for import or migrate)")
    return parser


def main() -> None:
    parser = args_parser()
    args = parser.parse_args()

    print_help = True
    if "export" == args.action and (args.cluster and args.connection_db) and (args.azure_cli_authentication or (args.aad_app_id and args.aad_app_secret and args.aad_tenant_id)):
        print_help = False
        export_util = KustoExportUtility(cluster=args.cluster, default_database=args.connection_db, az_cli_auth=args.azure_cli_authentication,
                                         aad_client_id=args.aad_app_id, aad_client_secret=args.aad_app_secret, aad_tenant_id=args.aad_tenant_id)
        export_util.start_export()

    if "import" == args.action and (args.cluster and args.target_cluster and args.subscription_id and args.resource_group and args.location) and (args.azure_cli_authentication or (args.aad_app_id and args.aad_app_secret and args.aad_tenant_id)):
        print_help = False
        import_util = KustoImportUtility(source_cluster=args.cluster, target_cluster=args.target_cluster, target_subscription_id=args.subscription_id, target_location=args.location,
                                         target_resource_group=args.resource_group, az_cli_auth=args.azure_cli_authentication, aad_client_id=args.aad_app_id, aad_client_secret=args.aad_app_secret, aad_tenant_id=args.aad_tenant_id)
        import_util.start_import()

    if "migrate" == args.action and (args.cluster and args.connection_db) and (args.target_cluster and args.subscription_id and args.resource_group and args.location) and (args.azure_cli_authentication or (args.aad_app_id and args.aad_app_secret and args.aad_tenant_id)):
        print_help = False

        export_util = KustoExportUtility(cluster=args.cluster, default_database=args.connection_db, az_cli_auth=args.azure_cli_authentication,
                                         aad_client_id=args.aad_app_id, aad_client_secret=args.aad_app_secret, aad_tenant_id=args.aad_tenant_id)
        export_util.start_export()

        import_util = KustoImportUtility(source_cluster=args.cluster, target_cluster=args.target_cluster, target_subscription_id=args.subscription_id, target_location=args.location,
                                         target_resource_group=args.resource_group, az_cli_auth=args.azure_cli_authentication, aad_client_id=args.aad_app_id, aad_client_secret=args.aad_app_secret, aad_tenant_id=args.aad_tenant_id)
        import_util.start_import()

    if print_help:
        parser.print_help()


if __name__ == "__main__":
    main()
