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
        file_name=f'appsflyer_push_data_{date}.csv',
        dropbox_path=f'/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/integrated_appsflyer push',
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
    )
    SELECT * FROM appsflyer_push_data
    '''
    )

    worker.work(attr=attr, info=info)
