from pathlib import Path
import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName
from worker.abstract_worker import Worker

class hyper_file_upload(Worker):

    def table_type_definition(self, num_list , text_list , date_list):

    # 테이블 타입 지정해주기
        table_definition = []

        for i in num_list:
            num_td = TableDefinition.Column(name= i, type=SqlType.numeric(precision=1, scale=0), nullability=NOT_NULLABLE)
            table_definition.append(num_td)

        for i in text_list:
            text_td = TableDefinition.Column(name=i, type=SqlType.text(), nullability=NOT_NULLABLE)
            table_definition.append(text_td)

        for i in date_list:
            date_td = TableDefinition.Column(name=i, type=SqlType.timestamp(), nullability=NOT_NULLABLE)
            table_definition.append(date_td)

        return table_definition

    def insert_data(self, hyper_name, table_definition ,data ,tableau_id , tableau_pw , tableau_sever, project_name):

        path_to_database = Path(hyper_name)

        # The table is called "Extract" and will be created in the "Extract" schema
        # and contains four columns.
        extract_table = TableDefinition(
            table_name=TableName("Extract", "Extract"),
            columns=table_definition
        )

        """
        An example demonstrating a simple single-table Hyper file including table creation and data insertion with different types
        This code is lifted from the below example:
        https://github.com/tableau/hyper-api-samples/blob/main/Tableau-Supported/Python/insert_data_into_single_table.py
        """
        print("Creating single table for publishing.")

        # Starts the Hyper Process with telemetry enabled to send data to Tableau.
        # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Creates new Hyper file "customer.hyper".
            # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
            with Connection(endpoint=hyper.endpoint,
                            database=path_to_database,
                            create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
                connection.catalog.create_schema(schema=extract_table.table_name.schema_name)
                connection.catalog.create_table(table_definition=extract_table)

                # The rows to insert into the "Extract"."Extract" table.
                data_to_insert = data

                with Inserter(connection, extract_table) as inserter:
                    #print(data_to_insert)
                    inserter.add_rows(rows=data_to_insert)
                    inserter.execute()

                # The table names in the "Extract" schema (the default schema).
                table_names = connection.catalog.get_table_names("Extract")
                print(f"Tables available in {path_to_database} are: {table_names}")

                # Number of rows in the "Extract"."Extract" table.
                # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
                row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {extract_table.table_name}")
                print(f"The number of rows in table {extract_table.table_name} is {row_count}.")

            print("The connection to the Hyper file has been closed.")
        print("The Hyper process has been shut down.")

        """
        Shows how to leverage the Tableau Server Client (TSC) to sign in and publish an extract directly to Tableau Online/Server
        """

        # Sign in to server
        tableau_auth = TSC.TableauAuth(tableau_id, tableau_pw)
        server = TSC.Server(tableau_sever, use_server_version=True)

        print(f"Signing into at {tableau_sever}")
        with server.auth.sign_in(tableau_auth):
            # Define publish mode - Overwrite, Append, or CreateNew
            publish_mode = TSC.Server.PublishMode.Overwrite

            # Get project_id from project_name
            all_projects, pagination_item = server.projects.get()
            for project in TSC.Pager(server.projects):
                if project.name == project_name:
                    project_id = project.id

            # Create the datasource object with the project_id
            datasource = TSC.DatasourceItem(project_id)

            print(f"Publishing {hyper_name} to {project_name}...")
            # Publish datasource
            datasource = server.datasources.publish(datasource, path_to_database, publish_mode)
            print("Datasource published. Datasource ID: {0}".format(datasource.id))

            return print('Tableau Hyper Upload Success')

    def do_work(self ,info:dict, attr:dict):

        hyper_name = info['hyper_name']
        tableau_id = attr['tableau_id']
        tableau_pw = attr['tableau_pw']
        tableau_sever = attr['tableau_sever']
        project_name = info['project_name']

        num_list = info['num_list']
        date_list = info['date_list']
        text_list = info['text_list']
        data = info['data']

        table_definition = self.table_type_definition(num_list,text_list,date_list)
        self.insert_data(hyper_name, table_definition ,data ,tableau_id, tableau_pw, tableau_sever, project_name)

        return "Tableau Hyper File Upload Success"
