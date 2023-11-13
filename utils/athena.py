import boto3
import time
from datetime import datetime, timedelta
import calendar
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


def athena_table_manually_refresh(database, table_name, table_s3_path, owner_id, channel, start_date: str):
    athena = boto3.client('athena', region_name='ap-northeast-2')
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    queries = []
    table = f"{database}.{table_name}"
    queries.extend(get_partitions(table, table_s3_path, owner_id, channel, start_date))
    try:
        # one query takes 20 to 500ms and queued queries limit is 25.
        for query in queries:
            res = athena.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': database
                },
                ResultConfiguration={
                    'OutputLocation': 's3://data-consulting-private/athena_refresh_result/'
                }
            )
            time.sleep(0.08)
    except Exception as e:
        raise e

    return 'OK'


def get_partitions(table, path, owner_id, channel, start_date):
    num_days = calendar.monthrange(start_date.year, start_date.month)[1]
    num_days = num_days - start_date.day + 1
    return map(
        lambda d: " ".join(
            """
            ALTER TABLE {table} ADD IF NOT EXISTS
            PARTITION (owner_id='{owner_id}', channel='{channel}', year={year}, month={month}, day={day}, hour={hour}, minute={minute})
            LOCATION 's3://{path}/owner_id={owner_id}/channel={channel}/year={year}/month={month_zf}/day={day_zf}/hour={hour_zf}/minute={minute_zf}/'
            """.format(
                table=table,
                path=path,
                owner_id=owner_id,
                channel=channel,
                year=d.year,
                month=d.month,
                month_zf=str(d.month).zfill(2),
                day=d.day,
                day_zf=str(d.day).zfill(2),
                hour=d.hour,
                hour_zf=str(d.hour).zfill(2),
                minute=d.minute,
                minute_zf=str(d.minute).zfill(2)
            ).split()
        ),
        map(
            lambda i: (start_date + timedelta(minutes=i)),
            range(0, num_days*24*60, 5)
        )
    )


def get_table_data_from_athena(database, query, source='result'):
    athena = boto3.client('athena', region_name='ap-northeast-2')
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
        result_df = pd.read_csv(f_path, encoding='utf-8-sig')

        os.remove(f_path)

    return result_df


# 1000행 이상의 결과값 추출 하는 경우 활용
def fetchall_athena(database, query):
    client = boto3.client('athena', region_name='ap-northeast-2')
    query_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://data-consulting-private/Unsaved/'
        }
    )['QueryExecutionId']

    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query))
        time.sleep(10)

    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )

    data_list = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data_list.append(row['Data'])

    column_names = [x['VarCharValue'] for x in data_list[0]]

    results = []
    for datum in data_list[1:]:
        results.append([x['VarCharValue'] if 'VarCharValue' in x.keys() else '' for x in datum])

    df = pd.DataFrame(results, columns=column_names)

    return df

