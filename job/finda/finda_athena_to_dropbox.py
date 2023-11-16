import datetime
from dateutil.relativedelta import relativedelta
from solution.DCT1739_athena_to_dropbox_solution import AthenaToDropbox


if __name__ == "__main__":
    worker = AthenaToDropbox(__file__)
    attr = dict(
        owner_id="finda"
    )
    owner_id = 'aurora_b102'
    today = datetime.datetime.now() - relativedelta(days=1)
    year = today.year
    month = today.month
    last_month = (today - relativedelta(months=1)).month
    last_day = (today.replace(day=1) - relativedelta(days=1)).day
    date = datetime.datetime.strftime(today, '%Y%m%d')
    info = dict(
        file_name=f'appsflyer_final_report_{date}.csv',
        dropbox_path=f'/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/integrated_appsflyer raw',
        source='result',
        query=f'''
    WITH data_set AS (
    SELECT temp.*,
        COALESCE(CAST(JSON_EXTRACT(temp.event_value, '$.af_revenue') AS INT), 0) AS event_revenue
    FROM (
        SELECT attributed_touch_type, attributed_touch_time, install_time, 
            event_time, event_name, event_value, partner, appsflyer_id, platform,
            media_source, channel, keywords, campaign, campaign_id, adset, adset_id, ad, ad_id,
            site_id, sub_site_id, sub_param_1, sub_param_2, sub_param_3, sub_param_4, sub_param_5, 
            is_retargeting, NULL AS is_primary_attribution, collected_at, year, month, day
        FROM dmp_athena.appsflyer_conversions_non_organic
        WHERE 
            owner_id = '{owner_id}'
            AND year = {year}
            AND ((month = {month}) OR (month = {last_month} AND day = {last_day}))
            AND attributed_touch_type = 'click'
            AND media_source IN 
            ('KA-FRIEND', 'kakao_int', 'googleadwords_int', 'Apple Search Ads', 'moloco_int', 'rtbhouse_int', 'appier_int',
            'cauly_int', 'nstation_int', 'nswitch_int', 'adisonofferwall_int', 'cashfriends_int', 'bobaedream', 'v3', 'remember',
            'encar', 'bytedanceglobal_int', 'naversd', 'tnk_int', 'toss', 'naver_int', 'blind', 'criteonew_int',
            'igaworkstradingworksvideo_int', 'inmobidsp_int', 'jobplanet_onelink', 'naverband', 'doohub_int', 'remerge_int'
            ,'igaworkstradingworks_int','xcloudgame_int','valista_int','leadgenetics_int','manplus_int','Facebook Ads'
            ,'Facebook_onelink','carrot','afreecatvda_int','inmobi_int','push')
        UNION ALL 
        SELECT attributed_touch_type, attributed_touch_time, install_time, 
            event_time, event_name, event_value, partner, appsflyer_id, platform, 
            media_source, channel, keywords, campaign, campaign_id, adset, adset_id, ad, ad_id,
            site_id, sub_site_id, sub_param_1, sub_param_2,  sub_param_3, sub_param_4, sub_param_5, 
            is_retargeting, NULL AS is_primary_attribution, collected_at, year, month, day
        FROM dmp_athena.appsflyer_installs_non_organic
        WHERE 
            owner_id = '{owner_id}'
            AND year = {year}
            AND ((month = {month}) OR (month = {last_month} AND day = {last_day}))
            AND attributed_touch_type = 'click'
            AND media_source IN 
            ('KA-FRIEND', 'kakao_int', 'googleadwords_int', 'Apple Search Ads', 'moloco_int', 'rtbhouse_int', 'appier_int',
            'cauly_int', 'nstation_int', 'nswitch_int', 'adisonofferwall_int', 'cashfriends_int', 'bobaedream', 'v3', 'remember',
            'encar', 'bytedanceglobal_int', 'naversd', 'tnk_int', 'toss', 'naver_int', 'blind', 'criteonew_int',
            'igaworkstradingworksvideo_int', 'inmobidsp_int', 'jobplanet_onelink', 'naverband', 'doohub_int', 'remerge_int'
            ,'igaworkstradingworks_int','xcloudgame_int','valista_int','leadgenetics_int','manplus_int','Facebook Ads'
            ,'Facebook_onelink','carrot','afreecatvda_int','inmobi_int','push')
        UNION ALL
        SELECT attributed_touch_type, attributed_touch_time, install_time, 
            event_time, event_name, event_value, partner, appsflyer_id, platform, 
            media_source, channel, keywords, campaign, campaign_id, adset, adset_id, ad, ad_id,
            site_id, sub_site_id, sub_param_1, sub_param_2,  sub_param_3, sub_param_4, sub_param_5, 
            is_retargeting, is_primary_attribution, collected_at, year, month, day
        FROM dmp_athena.appsflyer_re_events_non_organic
        WHERE 
            owner_id = '{owner_id}'
            AND year = {year}
            AND ((month = {month}) OR (month = {last_month} AND day = {last_day}))
            AND attributed_touch_type = 'click'
            AND media_source IN 
            ('KA-FRIEND', 'kakao_int', 'googleadwords_int', 'Apple Search Ads', 'moloco_int', 'rtbhouse_int', 'appier_int',
            'cauly_int', 'nstation_int', 'nswitch_int', 'adisonofferwall_int', 'cashfriends_int', 'bobaedream', 'v3', 'remember',
            'encar', 'bytedanceglobal_int', 'naversd', 'tnk_int', 'toss', 'naver_int', 'blind', 'criteonew_int',
            'igaworkstradingworksvideo_int', 'inmobidsp_int', 'jobplanet_onelink', 'naverband', 'doohub_int', 'remerge_int'
            ,'igaworkstradingworks_int','xcloudgame_int','valista_int','leadgenetics_int','manplus_int','Facebook Ads'
            ,'Facebook_onelink','carrot','afreecatvda_int','inmobi_int','push')
            AND is_primary_attribution = true
        UNION ALL
        SELECT attributed_touch_type, attributed_touch_time, install_time, 
            event_time, event_name, event_value, partner, appsflyer_id, platform, 
            media_source, channel, keywords, campaign, campaign_id, adset, adset_id, ad, ad_id,
            site_id, sub_site_id, sub_param_1, sub_param_2,  sub_param_3, sub_param_4, sub_param_5, 
            is_retargeting, is_primary_attribution, collected_at, year, month, day
        FROM dmp_athena.appsflyer_reinstalls_non_organic
        WHERE 
            owner_id = '{owner_id}'
            AND year = {year}
            AND ((month = {month}) OR (month = {last_month} AND day = {last_day}))
            AND attributed_touch_type = 'click'
            AND media_source IN 
            ('KA-FRIEND', 'kakao_int', 'googleadwords_int', 'Apple Search Ads', 'moloco_int', 'rtbhouse_int', 'appier_int',
            'cauly_int', 'nstation_int', 'nswitch_int', 'adisonofferwall_int', 'cashfriends_int', 'bobaedream', 'v3', 'remember',
            'encar', 'bytedanceglobal_int', 'naversd', 'tnk_int', 'toss', 'naver_int', 'blind', 'criteonew_int',
            'igaworkstradingworksvideo_int', 'inmobidsp_int', 'jobplanet_onelink', 'naverband', 'doohub_int', 'remerge_int'
            ,'igaworkstradingworks_int','xcloudgame_int','valista_int','leadgenetics_int','manplus_int','Facebook Ads'
            ,'Facebook_onelink','carrot','afreecatvda_int','inmobi_int','push')
            AND is_primary_attribution = true
        UNION ALL
        SELECT attributed_touch_type, attributed_touch_time, install_time, 
            event_time, event_name, event_value, partner, appsflyer_id, platform, 
            media_source, channel, keywords, campaign, campaign_id, adset, adset_id, ad, ad_id,
            site_id, sub_site_id, sub_param_1, sub_param_2,  sub_param_3, sub_param_4, sub_param_5, 
            is_retargeting, is_primary_attribution, collected_at, year, month, day
        FROM dmp_athena.appsflyer_ua_events_non_organic
        WHERE 
            owner_id = '{owner_id}'
            AND year = {year}
            AND ((month = {month}) OR (month = {last_month} AND day = {last_day}))
            AND attributed_touch_type = 'click'
            AND media_source IN 
            ('KA-FRIEND', 'kakao_int', 'googleadwords_int', 'Apple Search Ads', 'moloco_int', 'rtbhouse_int', 'appier_int',
            'cauly_int', 'nstation_int', 'nswitch_int', 'adisonofferwall_int', 'cashfriends_int', 'bobaedream', 'v3', 'remember',
            'encar', 'bytedanceglobal_int', 'naversd', 'tnk_int', 'toss', 'naver_int', 'blind', 'criteonew_int',
            'igaworkstradingworksvideo_int', 'inmobidsp_int', 'jobplanet_onelink', 'naverband', 'doohub_int', 'remerge_int'
            ,'igaworkstradingworks_int','xcloudgame_int','valista_int','leadgenetics_int','manplus_int','Facebook Ads'
            ,'Facebook_onelink','carrot','afreecatvda_int','inmobi_int','push')
            AND is_primary_attribution = true
        ) AS temp
    ),
    conversion_data AS (
        SELECT *, ROW_NUMBER() OVER(ORDER BY appsflyer_id, install_time, media_source_2, campaign) AS num
        FROM (
            SELECT *, CASE WHEN media_source = 'push' THEN '#push' ELSE media_source END media_source_2
            FROM data_set
        )
        WHERE event_name IN ('install', 're-engagement', 're-attribution')
    ),
    dedup_conversion_data AS (
        SELECT appsflyer_id, DATE_PARSE(install_time, '%Y-%m-%d %H:%i:%s') AS install_time, 
                event_time, event_name, event_value, event_revenue, partner, platform,
                media_source, channel, keywords, campaign, campaign_id, adset, adset_id, ad, ad_id,
                site_id, sub_site_id, sub_param_1, sub_param_2,  sub_param_3, sub_param_4, sub_param_5, 
                is_retargeting, is_primary_attribution, ROW_NUMBER() OVER(ORDER BY appsflyer_id, install_time, media_source_2) AS num
        FROM conversion_data
        WHERE num IN (
            SELECT MIN(num)
            FROM conversion_data
            GROUP BY appsflyer_id, install_time, media_source
            )
    ),
    push_data AS(
        SELECT 
            diff_data.appsflyer_id,
            diff_data.install_time,
            diff_data.diff,
            diff_data.last_install_time,
            t3.media_source AS conversion_source,
            t3.channel,
            t3.keywords,
            t3.campaign,
            t3.campaign_id,
            t3.adset,
            t3.adset_id,
            t3.ad,
            t3.ad_id,
            t3.site_id,
            t3.sub_site_id,
            t3.sub_param_1,
            t3.sub_param_2,
            t3.sub_param_3,
            t3.sub_param_4,
            t3.sub_param_5,
            t3.is_retargeting,
            t3.platform,
            t3.is_primary_attribution
        FROM (
            SELECT t1.appsflyer_id AS appsflyer_id, t1.install_time AS install_time, 
                t1.install_time - COALESCE(MAX(t2.install_time), t1.install_time) AS diff,
                MAX(t2.install_time) AS last_install_time, MAX(t2.num) AS num
            FROM dedup_conversion_data t1
            LEFT JOIN dedup_conversion_data t2 
            ON t1.appsflyer_id = t2.appsflyer_id AND t1.install_time > t2.install_time
            WHERE t1.media_source = 'push'
            GROUP BY t1.appsflyer_id, t1.install_time
        ) diff_data
        LEFT JOIN dedup_conversion_data t3
            ON diff_data.num = t3.num
        WHERE diff < INTERVAL '1' DAY
        ORDER BY appsflyer_id, install_time
    ),
    appsflyer_push_data AS (
        SELECT 
            d.attributed_touch_type, d.attributed_touch_time, d.install_time, 
            d.event_time, d.event_name, d.event_value, d.event_revenue, d.partner, d.appsflyer_id, d.platform, 
            d.collected_at, d.year, d.month, d.day,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.conversion_source ELSE d.media_source END AS media_source,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.channel ELSE d.channel END AS channel,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.keywords ELSE d.keywords END AS keywords,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.campaign ELSE d.campaign END AS campaign,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.campaign_id ELSE d.campaign_id END AS campaign_id,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.adset ELSE d.adset END AS adset,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.adset_id ELSE d.adset_id END AS adset_id,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.ad ELSE d.ad END AS ad,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.ad_id ELSE d.ad_id END AS ad_id,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.site_id ELSE d.site_id END AS site_id,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.sub_site_id ELSE d.sub_site_id END AS sub_site_id,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.sub_param_1 ELSE d.sub_param_1 END AS sub_param_1,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.sub_param_2 ELSE d.sub_param_2 END AS sub_param_2,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.sub_param_3 ELSE d.sub_param_3 END AS sub_param_3,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.sub_param_4 ELSE d.sub_param_4 END AS sub_param_4,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.sub_param_5 ELSE d.sub_param_5 END AS sub_param_5,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.is_retargeting ELSE d.is_retargeting END AS is_retargeting,
            CASE WHEN d.media_source = 'push' AND p.conversion_source IS NOT NULL THEN p.is_primary_attribution ELSE d.is_primary_attribution END AS is_primary_attribution
        FROM data_set d
        LEFT JOIN push_data p
            ON d.appsflyer_id = p.appsflyer_id 
            AND DATE_PARSE(d.install_time, '%Y-%m-%d %H:%i:%s') = p.install_time
        WHERE MONTH(DATE_PARSE(d.event_time, '%Y-%m-%d %H:%i:%s')) = {month}
    ), -- 1차 가공 완료
    prep_appsflyer_df AS (
        SELECT *,
            DATE_PARSE(event_time, '%Y-%m-%d %H:%i:%s') - DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s') AS CTET
        FROM (
            SELECT 
                attributed_touch_type, 
                CASE WHEN attributed_touch_time = '' THEN install_time ELSE attributed_touch_time END AS attributed_touch_time,
                install_time, event_time, event_name, event_value, event_revenue, partner, appsflyer_id, platform,
                1 AS cnt, media_source, channel, keywords, campaign, campaign_id, adset, adset_id, ad, ad_id,
                site_id, sub_site_id, sub_param_1, sub_param_2,  sub_param_3, sub_param_4, sub_param_5, 
                is_retargeting, is_primary_attribution, collected_at, year, month, day
            FROM appsflyer_push_data
            WHERE (
                media_source IN 
                ('KA-FRIEND', 'kakao_int', 'Apple Search Ads', 'moloco_int', 'rtbhouse_int', 'appier_int',
                'cauly_int', 'nstation_int', 'nswitch_int', 'adisonofferwall_int', 'cashfriends_int', 'bobaedream', 'v3', 'remember',
                'encar', 'bytedanceglobal_int', 'naversd', 'tnk_int', 'toss', 'naver_int', 'blind', 'criteonew_int',
                'igaworkstradingworksvideo_int', 'inmobidsp_int', 'jobplanet_onelink', 'naverband', 'doohub_int', 'remerge_int',
                'igaworkstradingworks_int','xcloudgame_int','valista_int','leadgenetics_int','manplus_int', 'carrot','afreecatvda_int','inmobi_int')
            OR (media_source = 'googleadwords_int' AND channel IN ('ACI_Search','ACE_Search','ACE_Youtube','ACE_Display','ACI_Youtube','ACI_Display'))
            OR (media_source IN ('Facebook Ads', 'Facebook_onelink') AND campaign NOT LIKE '%_RT_%' AND campaign != 'Madit_CNV_retargeting')
            )
            AND event_name IN 
                ('Clicked Signup Completion Button', 'install', 'loan_contract_completed', 'loan_contract_completed_fee', 
                're-attribution', 're-engagement', 'Viewed LA Home', 'Viewed LA Home No Result')
        )
    ),
    cn_table AS (
        SELECT 
            DATE(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) AS click_date,
            DATE(DATE_PARSE(event_time, '%Y-%m-%d %H:%i:%s')) AS event_date,
            campaign, adset, ad, channel, platform, is_retargeting,
            CASE WHEN event_name = 'install' AND CTET <= INTERVAL '2592000' SECOND THEN cnt ELSE 0  END AS "install(cn) D30",
            CASE WHEN event_name = 'loan_contract_completed_fee' AND CTET <= INTERVAL '2592000' SECOND THEN event_revenue ELSE 0 END AS "REVENUE(cn) D30",
            CASE WHEN event_name = 'loan_contract_completed_fee' AND CTET <= INTERVAL '648000' SECOND THEN event_revenue ELSE 0 END AS "REVENUE(cn) D7",
            CASE WHEN event_name = 'loan_contract_completed_fee' AND CTET <= INTERVAL '86400' SECOND THEN event_revenue ELSE 0 END AS "REVENUE(cn) D0",
            CASE WHEN event_name = 'loan_contract_completed_fee' AND CTET <= INTERVAL '2592000' SECOND THEN cnt ELSE 0 END AS "LOANFEE(cn) D30",
            CASE WHEN event_name = 'loan_contract_completed_fee' AND CTET <= INTERVAL '648000' SECOND THEN cnt ELSE 0 END AS "LOANFEE(cn) D7",
            CASE WHEN event_name = 'loan_contract_completed_fee' AND CTET <= INTERVAL '86400' SECOND THEN cnt ELSE 0 END AS "LOANFEE(cn) D0",
            CASE WHEN event_name = 'loan_contract_completed' AND CTET <= INTERVAL '2592000' SECOND THEN cnt ELSE 0 END AS "LOAN(cn) D30",
            CASE WHEN event_name = 'loan_contract_completed' AND CTET <= INTERVAL '648000' SECOND THEN cnt ELSE 0 END AS "LOAN(cn) D7",
            CASE WHEN event_name = 'loan_contract_completed' AND CTET <= INTERVAL '86400' SECOND THEN cnt ELSE 0 END AS "LOAN(cn) D0"
        FROM prep_appsflyer_df
        WHERE event_name IN ('install', 'loan_contract_completed', 'loan_contract_completed_fee')
    ),
    uni_D0_table AS (
        SELECT 
            num,
            DATE(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) AS click_date,
            DATE(DATE_PARSE(event_time, '%Y-%m-%d %H:%i:%s')) AS event_date,
            campaign, adset, ad, channel, platform, is_retargeting, event_name, appsflyer_id,
            CASE WHEN event_name = 'install' THEN cnt ELSE 0 END AS "install(uni) D0",
            CASE WHEN event_name = 're-attribution' THEN cnt ELSE 0 END AS "re-attribution(uni) D0",
            CASE WHEN event_name = 're-engagement' THEN cnt ELSE 0 END AS "re-engagement(uni) D0",
            CASE WHEN event_name = 'Clicked Signup Completion Button' THEN cnt ELSE 0 END AS "CS(uni) D0"
        FROM ( 
            SELECT *, ROW_NUMBER() OVER(ORDER BY event_time, install_time, attributed_touch_time, campaign) AS num
            FROM prep_appsflyer_df
            WHERE event_name IN ('install', 're-attribution', 're-engagement', 'Clicked Signup Completion Button')
            AND YEAR(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) = {year}
            AND MONTH(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) = {month}
            AND CTET <= INTERVAL '86400' SECOND
        )
    ),
    uni_vlh_D0_table AS (
        SELECT 
            num,
            DATE(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) AS click_date,
            DATE(DATE_PARSE(event_time, '%Y-%m-%d %H:%i:%s')) AS event_date,
            campaign, adset, ad, channel, platform, is_retargeting, event_name, appsflyer_id,
            CASE WHEN event_name = 'Viewed LA Home' THEN cnt ELSE 0 END AS "VLH(uni) D0",
            CASE WHEN event_name = 'Viewed LA Home No Result' THEN cnt ELSE 0 END AS "VLHN(uni) D0"
        FROM (
            SELECT *, ROW_NUMBER() OVER(ORDER BY event_time, install_time, attributed_touch_time, event_name) AS num
            FROM prep_appsflyer_df
            WHERE event_name IN ('Viewed LA Home', 'Viewed LA Home No Result')
            AND YEAR(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) = {year}
            AND MONTH(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) = {month}
            AND CTET <= INTERVAL '86400' SECOND
        )
    ),
    uni_cs_D30_table AS (
        SELECT 
            num,
            DATE(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) AS click_date,
            DATE(DATE_PARSE(event_time, '%Y-%m-%d %H:%i:%s')) AS event_date,
            campaign, adset, ad, channel, platform, is_retargeting, event_name, appsflyer_id,
            CASE WHEN event_name = 'Clicked Signup Completion Button' THEN cnt ELSE 0 END AS "CS(uni) D30"
        FROM (
            SELECT *, ROW_NUMBER() OVER(ORDER BY event_time, install_time, attributed_touch_time, campaign) AS num
            FROM prep_appsflyer_df
            WHERE event_name IN ('Clicked Signup Completion Button')
            AND CTET <= INTERVAL '2592000' SECOND
        )
    ),
    uni_vlh_D30_table AS (
        SELECT 
            num,
            DATE(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) AS click_date,
            DATE(DATE_PARSE(event_time, '%Y-%m-%d %H:%i:%s')) AS event_date,
            campaign, adset, ad, channel, platform, is_retargeting, event_name, appsflyer_id,
            CASE WHEN event_name = 'Viewed LA Home' THEN cnt ELSE 0 END AS "VLH(uni) D30",
            CASE WHEN event_name = 'Viewed LA Home No Result' THEN cnt ELSE 0 END AS "VLHN(uni) D30"
        FROM (
            SELECT *, ROW_NUMBER() OVER(ORDER BY event_time, install_time, attributed_touch_time, event_name) AS num
            FROM prep_appsflyer_df
            WHERE event_name IN ('Viewed LA Home', 'Viewed LA Home No Result')
            AND YEAR(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) = {year}
            AND MONTH(DATE_PARSE(attributed_touch_time, '%Y-%m-%d %H:%i:%s')) = {month}
            AND CTET <= INTERVAL '2592000' SECOND
        )
    ),
    final_apps_data AS (
        SELECT 
            COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t1."date", t2."date"), t3."date"), t4."date"), t5."date"), t6."date") AS "date", 
            COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t1.campaign, t2.campaign), t3.campaign), t4.campaign), t5.campaign), t6.campaign) AS campaign, 
            COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t1.adset, t2.adset), t3.adset), t4.adset), t5.adset), t6.adset) AS adset, 
            COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t1.ad, t2.ad), t3.ad), t4.ad), t5.ad), t6.ad) AS ad, 
            COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t1.channel, t2.channel), t3.channel), t4.channel), t5.channel), t6.channel) AS channel, 
            COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t1.platform, t2.platform), t3.platform), t4.platform), t5.platform), t6.platform) AS platform, 
            COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(t1.is_retargeting, t2.is_retargeting), t3.is_retargeting), t4.is_retargeting), t5.is_retargeting), t6.is_retargeting) AS is_retargeting, 
            COALESCE("install(cn) D30", 0) AS "install(cn) D30", 
            COALESCE("REVENUE(cn) D30", 0) AS "REVENUE(cn) D30",
            COALESCE("REVENUE(cn) D30.T", 0) AS "REVENUE(cn) D30.T", 
            COALESCE("REVENUE(cn) D7.T", 0) AS "REVENUE(cn) D7.T", 
            COALESCE("REVENUE(cn) D0.T", 0) AS "REVENUE(cn) D0.T", 
            COALESCE("LOANFEE(cn) D30.T", 0) AS "LOANFEE(cn) D30.T", 
            COALESCE("LOANFEE(cn) D7.T", 0) AS "LOANFEE(cn) D7.T", 
            COALESCE("LOANFEE(cn) D0.T", 0) AS "LOANFEE(cn) D0.T",
            COALESCE("LOAN(cn) D30.T", 0) AS "LOAN(cn) D30.T", 
            COALESCE("LOAN(cn) D7.T", 0) AS "LOAN(cn) D7.T", 
            COALESCE("LOAN(cn) D0.T", 0) AS "LOAN(cn) D0.T",
            COALESCE("CS(uni) D30", 0) AS "CS(uni) D30",
            COALESCE("CS(uni) D0.T", 0) AS "CS(uni) D0.T",
            COALESCE("install(uni) D0.T", 0) AS "install(uni) D0.T",
            COALESCE("re-attribution(uni) D0.T", 0) AS "re-attribution(uni) D0.T", 
            COALESCE("re-engagement(uni) D0.T", 0) AS "re-engagement(uni) D0.T", 
            COALESCE("total_install(uni) D0.T", 0) AS "total_install(uni) D0.T",
            COALESCE("VLH(uni) D30.T", 0) AS "VLH(uni) D30.T", 
            COALESCE("VLHN(uni) D30.T", 0) AS "VLHN(uni) D30.T", 
            COALESCE("VLH+N(uni) D0.T", 0) AS "VLH+N(uni) D0.T"
        FROM (
            SELECT 
                event_date AS "date", campaign, adset, ad, channel, platform, is_retargeting,
                SUM("install(cn) D30") AS "install(cn) D30",
                SUM("REVENUE(cn) D30") AS "REVENUE(cn) D30"
            FROM cn_table
            GROUP BY event_date, campaign, adset, ad, channel, platform, is_retargeting
        ) t1
        FULL JOIN (
            SELECT 
                click_date AS "date", campaign, adset, ad, channel, platform, is_retargeting,
                SUM("REVENUE(cn) D30") AS "REVENUE(cn) D30.T",
                SUM("REVENUE(cn) D7") AS "REVENUE(cn) D7.T",
                SUM("REVENUE(cn) D0") AS "REVENUE(cn) D0.T",
                SUM("LOANFEE(cn) D30") AS "LOANFEE(cn) D30.T",
                SUM("LOANFEE(cn) D7") AS "LOANFEE(cn) D7.T",
                SUM("LOANFEE(cn) D0") AS "LOANFEE(cn) D0.T",
                SUM("LOAN(cn) D30") AS "LOAN(cn) D30.T",
                SUM("LOAN(cn) D7") AS "LOAN(cn) D7.T",
                SUM("LOAN(cn) D0") AS "LOAN(cn) D0.T"
            FROM cn_table
            WHERE YEAR(click_date) = {year} 
            AND MONTH(click_date) = {month}
            GROUP BY click_date, campaign, adset, ad, channel, platform, is_retargeting
        ) t2 ON t1."date" = t2."date" 
            AND t1.campaign = t2.campaign 
            AND t1.adset = t2.adset
            AND t1.ad = t2.ad
            AND t1.channel = t2.channel
            AND t1.platform = t2.platform
            AND t1.is_retargeting = t2.is_retargeting
        FULL JOIN (
            SELECT 
                event_date AS "date", campaign, adset, ad, channel, platform, is_retargeting,
                SUM("CS(uni) D30") AS "CS(uni) D30"
            FROM uni_cs_D30_table
            WHERE num IN (
                SELECT MIN(num)
                FROM uni_cs_D30_table
                GROUP BY appsflyer_id, event_name
                ) 
            GROUP BY event_date, campaign, adset, ad, channel, platform, is_retargeting
        ) t3 ON t1."date" = t3."date" 
            AND t1.campaign = t3.campaign 
            AND t1.adset = t3.adset
            AND t1.ad = t3.ad
            AND t1.channel = t3.channel
            AND t1.platform = t3.platform
            AND t1.is_retargeting = t3.is_retargeting
        FULL JOIN (
            SELECT 
                click_date AS "date", campaign, adset, ad, channel, platform, is_retargeting,
                SUM("VLH(uni) D30") AS "VLH(uni) D30.T",
                SUM("VLHN(uni) D30") AS "VLHN(uni) D30.T"
            FROM uni_vlh_D30_table
            WHERE num IN (
                SELECT MIN(num)
                FROM uni_vlh_D30_table
                GROUP BY appsflyer_id, event_name
                ) 
            GROUP BY click_date, campaign, adset, ad, channel, platform, is_retargeting
        ) t4 ON t1."date" = t4."date" 
            AND t1.campaign = t4.campaign 
            AND t1.adset = t4.adset
            AND t1.ad = t4.ad
            AND t1.channel = t4.channel
            AND t1.platform = t4.platform
            AND t1.is_retargeting = t4.is_retargeting
        FULL JOIN (
            SELECT 
                click_date AS "date", campaign, adset, ad, channel, platform, is_retargeting,
                SUM("CS(uni) D0") AS "CS(uni) D0.T",
                SUM("install(uni) D0") AS "install(uni) D0.T",
                SUM("re-attribution(uni) D0") AS "re-attribution(uni) D0.T",
                SUM("re-engagement(uni) D0") AS "re-engagement(uni) D0.T",
                SUM("install(uni) D0") + SUM("re-attribution(uni) D0") + SUM("re-engagement(uni) D0") AS "total_install(uni) D0.T"
            FROM uni_D0_table
            WHERE num IN (
                SELECT MIN(num)
                FROM uni_D0_table
                GROUP BY appsflyer_id, event_name
                ) 
            GROUP BY click_date, campaign, adset, ad, channel, platform, is_retargeting
        ) t5 ON t1."date" = t5."date" 
            AND t1.campaign = t5.campaign 
            AND t1.adset = t5.adset
            AND t1.ad = t5.ad
            AND t1.channel = t5.channel
            AND t1.platform = t5.platform
            AND t1.is_retargeting = t5.is_retargeting
        FULL JOIN (
            SELECT 
                click_date AS "date", campaign, adset, ad, channel, platform, is_retargeting,
                SUM("VLH(uni) D0") + SUM("VLHN(uni) D0") AS "VLH+N(uni) D0.T"
            FROM uni_vlh_D0_table
            WHERE num IN (
                SELECT MIN(num)
                FROM uni_vlh_D0_table
                GROUP BY appsflyer_id
                ) 
            GROUP BY click_date, campaign, adset, ad, channel, platform, is_retargeting
        ) t6 ON t1."date" = t6."date" 
            AND t1.campaign = t6.campaign 
            AND t1.adset = t6.adset
            AND t1.ad = t6.ad
            AND t1.channel = t6.channel
            AND t1.platform = t6.platform
            AND t1.is_retargeting = t6.is_retargeting
    )
    SELECT "date", "campaign", "adset", "ad", "channel", "platform", "is_retargeting", 
           SUM("install(cn) D30") AS "install(cn) D30", 
           SUM("REVENUE(cn) D30") AS "REVENUE(cn) D30",
           SUM("REVENUE(cn) D30.T") AS "REVENUE(cn) D30.T", 
           SUM("REVENUE(cn) D7.T") AS "REVENUE(cn) D7.T",
           SUM("REVENUE(cn) D0.T") AS "REVENUE(cn) D0.T",
           SUM("LOANFEE(cn) D30.T") AS "LOANFEE(cn) D30.T", 
           SUM("LOANFEE(cn) D7.T") AS "LOANFEE(cn) D7.T", 
           SUM("LOANFEE(cn) D0.T") AS "LOANFEE(cn) D0.T",
           SUM("LOAN(cn) D30.T") AS "LOAN(cn) D30.T", 
           SUM("LOAN(cn) D7.T") AS "LOAN(cn) D7.T", 
           SUM("LOAN(cn) D0.T") AS "LOAN(cn) D0.T", 
           SUM("CS(uni) D30") AS "CS(uni) D30",
           SUM("CS(uni) D0.T") AS "CS(uni) D0.T",
           SUM("install(uni) D0.T") AS "install(uni) D0.T", 
           SUM("re-attribution(uni) D0.T") AS "re-attribution(uni) D0.T",
           SUM("re-engagement(uni) D0.T") AS "re-engagement(uni) D0.T", 
           SUM("total_install(uni) D0.T") AS "total_install(uni) D0.T", 
           SUM("VLH(uni) D30.T") AS "VLH(uni) D30.T",
           SUM("VLHN(uni) D30.T") AS "VLHN(uni) D30.T", 
           SUM("VLH+N(uni) D0.T") AS "VLH+N(uni) D0.T"
    FROM 
    (SELECT 
        "date", "campaign", "adset", "ad",
        CASE WHEN "channel" IN ('ACE_Display', 'ACE_Youtube', 'ACE_Search', 'ACI_Display', 'ACI_Youtube', 'ACI_Search') THEN "channel" 
             ELSE '' 
        END AS "channel",
        "platform", "is_retargeting",
        "install(cn) D30",
        "REVENUE(cn) D30",
        "REVENUE(cn) D30.T",
        "REVENUE(cn) D7.T",
        "REVENUE(cn) D0.T",
        "LOANFEE(cn) D30.T",
        "LOANFEE(cn) D7.T",
        "LOANFEE(cn) D0.T",
        "LOAN(cn) D30.T",
        "LOAN(cn) D7.T",
        "LOAN(cn) D0.T",
        "CS(uni) D30",
        "CS(uni) D0.T",
        "install(uni) D0.T",
        "re-attribution(uni) D0.T",
        "re-engagement(uni) D0.T",
        "total_install(uni) D0.T",
        "VLH(uni) D30.T",
        "VLHN(uni) D30.T",
        "VLH+N(uni) D0.T"
    FROM final_apps_data)
    GROUP BY "date", "campaign", "adset", "ad", "channel", "platform", "is_retargeting"
    '''
    )

    worker.work(attr=attr, info=info)
