import pandas as pd
from pandas_gbq import read_gbq
from google.cloud import storage
from google.oauth2 import service_account
from multiprocessing import Pool


def get_repurchase_rate(df, result_dir, client, bucket_name):
    purchase_df = df.loc[df['event_name'].isin(['af_purchase', 'af_first_purchase'])].reset_index(drop=True)
    purchase_df['event_name'] = 'purchase'
    purchase_df['date'] = pd.to_datetime(purchase_df['event_time']).dt.date
    pivot_df = purchase_df.pivot_table(index=['date', 'appsflyer_id'], aggfunc='count')
    pivot_df = pivot_df.reset_index()[['date', 'appsflyer_id', 'event_name']]
    bucket = client.get_bucket(bucket_name)
    blob2 = bucket.blob(f'{result_dir}/purchase_log.csv')
    blob2.upload_from_string(pivot_df.to_csv(index=False, encoding='utf-8-sig'), content_type='text/csv')


def batch_query(query_with_offset):
    service_account_keyfile = "copper-sol-393307-47c2cfef1d7e.json"
    credentials = service_account.Credentials.from_service_account_file((service_account_keyfile))
    return read_gbq(query_with_offset, project_id="copper-sol-393307", credentials=credentials)


def parallel_batch_query(query, batch_size, num_processes):
    start_row = 0
    results = []

    with Pool(processes=num_processes) as pool:
        while True:
            # 쿼리 결과를 가져오는 작업을 병렬로 처리
            queries_with_offset = [f"{query} LIMIT {batch_size} OFFSET {start_row + i * batch_size}" for i in range(num_processes)]
            batch_results = pool.map(batch_query, queries_with_offset)

            # 결과가 없으면 반복문 종료
            if all(df.empty for df in batch_results):
                break

            # 결과를 결과 리스트에 추가
            results.extend(batch_results)

            # 다음 배치를 위해 start_row 증가
            start_row += batch_size * num_processes

    # 모든 배치 결과를 하나의 데이터프레임으로 병합
    final_result = pd.concat(results)

    return final_result


if __name__ == '__main__':
    get_total_df_query = """
    SELECT * FROM
    (SELECT `Event Time`, `Event Name`, `Event Value`, `Event Revenue`, `Event Revenue KRW`, `AppsFlyer ID`, "False" is_paid
    FROM copper-sol-393307.thehandsome.organic
    UNION ALL
    SELECT `Event Time`, `Event Name`, `Event Value`, `Event Revenue`, `Event Revenue KRW`, `AppsFlyer ID`,"True" is_paid
    FROM copper-sol-393307.thehandsome.paid)
    """
    test_query = """
    SELECT `Event Time`, `Event Name`, `Event Value`, `Event Revenue`, `Event Revenue KRW`, `AppsFlyer ID`, "False" is_paid
    FROM copper-sol-393307.thehandsome.paid
    WHERE `Event Time` > "2023-06-26"
    """
    # 배치 크기 설정
    batch_size = 1000000
    num_processes = 10
    total_df = parallel_batch_query(get_total_df_query, batch_size, num_processes)
    cols = list(total_df.columns)
    rename_cols = {}
    for col in cols:
        rename_cols[col] = col.lower().replace(' ', '_')
    total_df = total_df.rename(columns=rename_cols).drop_duplicates(ignore_index=True)
    total_df = total_df.fillna('')
    service_account_keyfile = "copper-sol-393307-47c2cfef1d7e.json"
    # 데이터프레임을 CSV 파일로 저장하여 GCS에 업로드
    client = storage.Client.from_service_account_json(service_account_keyfile)
    bucket_name = 'dataflow-apache-quickstart_copper-sol-393307'
    result_dir = f'더한섬/result_data'
    get_repurchase_rate(total_df, result_dir, client, bucket_name)


