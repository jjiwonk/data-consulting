from solution.DCT813_finda_gclid_solution import clickconversion_upload

if __name__ == "__main__":
    worker = clickconversion_upload(__file__)
    attr = dict(
        owner_id="finda"
    )
    info = dict(
        customer_id ='4983132762',
        error_slack_channel='수정 예정',
        success_alert_channel = 'pjt_dc_success'
    )

    worker.work(attr=attr, info=info)