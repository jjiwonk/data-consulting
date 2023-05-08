from utils.google_ads import get_google_client
from worker.abstract_worker import Worker

class clickconversion_upload(Worker):

    def send_conversion(self ,data ,owner_id ,customer_id):

        key = data.to_dict('index')
        client = get_google_client(owner_id)

        request = client.get_type("UploadClickConversionsRequest")
        request.customer_id = customer_id

        for i in range(len(data)):
            conversion_action_id = key[i].get('conversion_action_id')

            gclid = key[i].get('gclid')
            gbraid = key[i].get('gbraid')
            wbraid = key[i].get('wbraid')

            conversion_date_time = key[i].get('conversion_date_time')
            conversion_value = key[i].get('conversion_value')

            click_conversion = client.get_type("ClickConversion")

            conversion_upload_service = client.get_service("ConversionUploadService")
            conversion_action_service = client.get_service("ConversionActionService")

            click_conversion.conversion_action = conversion_action_service.conversion_action_path(
                customer_id, conversion_action_id
            )

            # Sets the single specified ID field.
            if gclid != '':
                click_conversion.gclid = gclid
            elif gbraid != '':
                click_conversion.gbraid = gbraid
            else:
                click_conversion.wbraid = wbraid

            click_conversion.conversion_date_time = conversion_date_time
            click_conversion.conversion_value = float(conversion_value)
            click_conversion.currency_code = "USD"

            request.conversions.append(click_conversion)

        request.partial_failure = True
        conversion_upload_response = conversion_upload_service.upload_click_conversions(
            request=request,
        )

        return print(f'Google Click ID {len(list(conversion_upload_response.results))}개 업로드 완료')

        # [END upload_offline_conversion]

    def do_work(self ,info:dict, attr:dict):

        owner_id = attr['owner_id']
        customer_id = info['customer_id']
        data = info['data']

        self.send_conversion(data,owner_id,customer_id)

        return "Click Conversion Upload Success"