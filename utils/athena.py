import boto3
import time
import pandas as pd
from utils import s3
from utils import const
from utils.path_util import get_tmp_path
import os

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

def get_table_data_from_athena(database, query, source= 'result'):
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

    if source == 'result':
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

    elif source == 's3' :
        s3_file = res['QueryExecutionId']
        s3_path = f'Unsaved/{s3_file}.csv'

        tmp_path = get_tmp_path() + f"/athena/"
        os.makedirs(tmp_path, exist_ok=True)

        f_path = s3.download_file(s3_path=s3_path, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET, local_path=tmp_path)
        result_df = pd.read_csv(f_path , encoding= 'utf-8-sig')

        os.remove(f_path)

    return result_df

