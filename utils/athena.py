import boto3
import time
import pandas as pd

def execute_query(athena, res):
    while True :
        try :
            time.sleep(5)
            result = athena.get_query_results(QueryExecutionId=res['QueryExecutionId'])
        except Exception as e :
            err_response = getattr(e, 'response', None)
            if err_response is None :
                print(e)
                raise(e)
            if err_response['Error']['Message'].__contains__('Could not find results'):
                return
            elif err_response['Error']['Message'].__contains__('Query has not yet finished') or err_response['Error']['Message'].__contains__("Rate exceeded"):
                time.sleep(5)
                continue
            print(e)
            raise (e)

        return result

def athena_table_refresh(database, table_name):
    athena = boto3.client('athena', region_name = 'ap-northeast-2')
    res = athena.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {database}.{table_name}",
        QueryExecutionContext={
            'Database': database,
        },
        ResultConfiguration={
            'OutputLocation': 's3://data-consulting-private/Unsaved/'
        }
    )

    return execute_query(athena, res)

def get_table_data_from_athena(database, query) :
    athena = boto3.client('athena', region_name = 'ap-northeast-2')
    res = athena.start_query_execution(
        QueryString= query,
        QueryExecutionContext={
            'Database': database,
        },
        ResultConfiguration={
            'OutputLocation': 's3://data-consulting-private/Unsaved/'
        }
    )

    result = execute_query(athena, res)
    columns = [info['Name'] for info in result['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    listed_results = []
    for res in result['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(' '))

        listed_results.append(dict(zip(columns, values)))

    result_df = pd.DataFrame(listed_results, columns=columns)

    return result_df