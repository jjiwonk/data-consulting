import pandas as pd
import datetime

class BigQueryPrep():
    def __init__(self):
        self.value_type_list = ['string_value', 'int_value']


    def read_data(self, raw_dir, json_file_name):
        data = pd.read_json(raw_dir + '/' + json_file_name, lines=True)
        data = data.reset_index()
        data = data.rename(columns={'index': 'event_id'})
        return data

    def timestamp_to_kst(self, df):
        df['event_timestamp_parsing'] = df['event_timestamp'].apply(
            lambda x: float(str(x)[:10] + "." + str(x)[-6:]))
        df['event_date_utc'] = df['event_timestamp_parsing'].apply(
            lambda x: datetime.datetime.fromtimestamp(x, ))
        df['event_date_kst'] = df['event_date_utc'].apply(lambda x: x + datetime.timedelta(hours=9))
        df['event_date'] = df['event_date_kst'].dt.date
        df = df.sort_values('event_timestamp')
        return df
    def event_param_parser(self, df, event_name):
        event_df = df.loc[df['event_name'] == event_name]
        event_df.index = range(len(event_df))

        series_list = []
        for data in event_df['event_params']:
            temp_dict = {}
            for item in data:
                for value_type in self.value_type_list:
                    if value_type in item['value']:
                        value = item['value'][value_type]
                        break
                    else:
                        pass

                temp_dict[item['key']] = value
            series_list.append(pd.Series(temp_dict))

        param_data = pd.DataFrame(series_list)

        event_df = pd.concat([event_df, param_data], axis=1)
        return event_df