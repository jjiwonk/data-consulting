import boto3
import time

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