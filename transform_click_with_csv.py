# -*- coding: utf-8 -*-
from collections import OrderedDict
import math
import sys
import csv
import time


cols = [
'created_at',
'sdk_verison',
'audience_session_id',
'audience_can_ad_id',
'audience_is_lat',
'third_party_gender',
'third_party_birth_year',
'third_party_married',
'third_party_registered_date',
'third_party_recent_activity_date',
'device_manufacturer',
'device_brand',
'device_os',
'device_os_version',
'ip_address',
'ip_region_code',
'ip_country_code',
'ip_city_code',
'ip_timezone',
'inventory_pub_id',
'inventory_pub_app_id',
'inventory_inventory_id',
'advertisement_advertiser_id',
'advertisement_campaign_id',
'advertisement_group_id',
'advertisement_content_id',
'advertisement_bid_price',
'advertisement_bid_strategy',
'click_decided_price',
'click_accum_impressions',
'lead_time',
'click_x_axis',
'click_y_axis',
'click_impression_id',
'interest1',
'interest2',
'interest3',
'interest4',
'interest5',
'interest6',
'interest7',
'interest8',
'interest9',
'interest10',
'interest11',
'interest12',
'interest13',
'interest14',
'interest15',
'interest16',
'interest17',
'interest18',
'interest19',
'interest20',
'interest21',
'interest22',
'interest23',
'interest24',
'interest25',
'interest26',
'interest27',
'interest28',
'interest29',
'interest30',
'interest31',
'interest32',
'interest33',
'interest34',
'interest35',
'interest36',
'interest37',
'interest38',
'interest39',
'interest40',
'interest41']


def tf_redshift_format(filename):
    # df = pandas.read_csv(filename)
    modified_df = []
    with open(filename, mode='r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:

            m_row = OrderedDict()

            m_row['created_at'] = row['meta_created_date'] + ' ' + row['meta_created_time']
            m_row['sdk_verison'] = row['meta_sdk_version']
            m_row['audience_session_id'] = row['audience_session_id']
            m_row['audience_can_ad_id'] = row['audience_can_ad_id']
            m_row['audience_is_lat'] = row['audience_is_lat']
            m_row['third_party_gender'] = row['audience_third_party_0_svc_gender']
            m_row['third_party_birth_year'] = row['audience_third_party_0_svc_birth_year']
            m_row['third_party_married'] = row['audience_third_party_0_svc_married']
            m_row['third_party_registered_date'] = row['audience_third_party_0_registered_date']
            m_row['third_party_recent_activity_date'] = row['audience_third_party_0_recent_activities_date']
            m_row['device_manufacturer'] = row['device_manufacturer']
            m_row['device_brand'] = row['device_brand_name']
            m_row['device_os'] = row['device_os_platform']
            m_row['device_os_version'] = row['device_os_version']
            m_row['ip_address'] = row['device_ip']
            m_row['ip_region_code'] = row['geo_ip_continent']
            m_row['ip_country_code'] = row['geo_ip_country']
            m_row['ip_city_code'] = row['geo_ip_city']
            m_row['ip_timezone'] = row['geo_ip_timezone']
            m_row['inventory_pub_id'] = row['inventory_pub_id']
            m_row['inventory_pub_app_id'] = row['inventory_pub_app_id']
            m_row['inventory_inventory_id'] = row['inventory_inventory_id']
            m_row['advertisement_advertiser_id'] = row['advertisement_advertiser_id']
            m_row['advertisement_campaign_id'] = row['advertisement_campaign_id']
            m_row['advertisement_group_id'] = row['advertisement_group_id']
            m_row['advertisement_content_id'] = row['advertisement_content_id']
            m_row['advertisement_bid_price'] = row['advertisement_bid_price']
            m_row['advertisement_bid_strategy'] = row['advertisement_bid_strategy_cd']
	    m_row['click_decided_price'] = row['ad_click_decided_price']
	    m_row['click_accum_impressions'] = row['ad_click_accum_grp_impressions']
	    m_row['lead_time'] = row['ad_click_lead_time']
	    m_row['click_x_axis'] = row['ad_click_click_pos_x']
	    m_row['click_y_axis'] = row['ad_click_click_pos_y']
	    m_row['click_impression_id'] = row['ad_click_impression_id']

            try:
                m_row['interest1'] =  int(row['audience_interest_apps_1'])  if not math.isnan(row['audience_interest_apps_1']) else 0
            except Exception:
                m_row['interest1'] = 0

            try:
                m_row['interest2'] =  int(row['audience_interest_apps_2'])  if not math.isnan(row['audience_interest_apps_2']) else 0 
            except Exception:
                m_row['interest2'] = 0

            try:
                m_row['interest3'] =  int(row['audience_interest_apps_3'])  if not math.isnan(row['audience_interest_apps_3']) else 0 
            except Exception:
                m_row['interest3'] = 0

            try:
                m_row['interest4'] =  int(row['audience_interest_apps_4'])  if not math.isnan(row['audience_interest_apps_4']) else 0 
            except Exception:
                m_row['interest4'] = 0

            try:
                m_row['interest5'] =  int(row['audience_interest_apps_5'])  if not math.isnan(row['audience_interest_apps_5']) else 0 
            except Exception:
                m_row['interest5'] = 0

            try:
                m_row['interest6'] =  int(row['audience_interest_apps_6'])  if not math.isnan(row['audience_interest_apps_6']) else 0 
            except Exception:
                m_row['interest6'] = 0

            try:
                m_row['interest7'] =  int(row['audience_interest_apps_7'])  if not math.isnan(row['audience_interest_apps_7']) else 0 
            except Exception:
                m_row['interest7'] = 0

            try:
                m_row['interest8'] =  int(row['audience_interest_apps_8'])  if not math.isnan(row['audience_interest_apps_8']) else 0 
            except Exception:
                m_row['interest8'] = 0

            try:
                m_row['interest9'] =  int(row['audience_interest_apps_9'])  if not math.isnan(row['audience_interest_apps_9']) else 0 
            except Exception:
                m_row['interest9'] = 0

            try:
                m_row['interest10'] = int(row['audience_interest_apps_10']) if not math.isnan(row['audience_interest_apps_10'])  else 0
            except Exception:
                m_row['interest10'] = 0

            try:
                m_row['interest11'] = int(row['audience_interest_apps_11']) if not math.isnan(row['audience_interest_apps_11'])  else 0
            except Exception:
                m_row['interest11'] = 0

            try:
                m_row['interest12'] = int(row['audience_interest_apps_12']) if not math.isnan(row['audience_interest_apps_12'])  else 0
            except Exception:
                m_row['interest12'] = 0

            try:
                m_row['interest13'] = int(row['audience_interest_apps_13']) if not math.isnan(row['audience_interest_apps_13'])  else 0
            except Exception:
                m_row['interest13'] = 0

            try:
                m_row['interest14'] = int(row['audience_interest_apps_14']) if not math.isnan(row['audience_interest_apps_14'])  else 0
            except Exception:
                m_row['interest14'] = 0

            try:
                m_row['interest15'] = int(row['audience_interest_apps_15']) if not math.isnan(row['audience_interest_apps_15'])  else 0
            except Exception:
                m_row['interest15'] = 0

            try:
                m_row['interest16'] = int(row['audience_interest_apps_16']) if not math.isnan(row['audience_interest_apps_16'])  else 0
            except Exception:
                m_row['interest16'] = 0

            try:
                m_row['interest17'] = int(row['audience_interest_apps_17']) if not math.isnan(row['audience_interest_apps_17'])  else 0
            except Exception:
                m_row['interest17'] = 0

            try:
                m_row['interest18'] = int(row['audience_interest_apps_18']) if not math.isnan(row['audience_interest_apps_18'])  else 0
            except Exception:
                m_row['interest18'] = 0

            try:
                m_row['interest19'] = int(row['audience_interest_apps_19']) if not math.isnan(row['audience_interest_apps_19'])  else 0
            except Exception:
                m_row['interest19'] = 0

            try:
                m_row['interest20'] = int(row['audience_interest_apps_20']) if not math.isnan(row['audience_interest_apps_20'])  else 0
            except Exception:
                m_row['interest20'] = 0

            try:
                m_row['interest21'] = int(row['audience_interest_apps_21']) if not math.isnan(row['audience_interest_apps_21'])  else 0
            except Exception:
                m_row['interest21'] = 0

            try:
                m_row['interest22'] = int(row['audience_interest_apps_22']) if not math.isnan(row['audience_interest_apps_22'])  else 0
            except Exception:
                m_row['interest22'] = 0

            try:
                m_row['interest23'] = int(row['audience_interest_apps_23']) if not math.isnan(row['audience_interest_apps_23'])  else 0
            except Exception:
                m_row['interest23'] = 0

            try:
                m_row['interest24'] = int(row['audience_interest_apps_24']) if not math.isnan(row['audience_interest_apps_24'])  else 0
            except Exception:
                m_row['interest24'] = 0

            try:
                m_row['interest25'] = int(row['audience_interest_apps_25']) if not math.isnan(row['audience_interest_apps_25'])  else 0
            except Exception:
                m_row['interest25'] = 0

            try:
                m_row['interest26'] = int(row['audience_interest_apps_26']) if not math.isnan(row['audience_interest_apps_26'])  else 0
            except Exception:
                m_row['interest26'] = 0

            try:
                m_row['interest27'] = int(row['audience_interest_apps_27']) if not math.isnan(row['audience_interest_apps_27'])  else 0
            except Exception:
                m_row['interest27'] = 0

            try:
                m_row['interest28'] = int(row['audience_interest_apps_28']) if not math.isnan(row['audience_interest_apps_28'])  else 0
            except Exception:
                m_row['interest28'] = 0

            try:
                m_row['interest29'] = int(row['audience_interest_apps_29']) if not math.isnan(row['audience_interest_apps_29'])  else 0
            except Exception:
                m_row['interest29'] = 0

            try:
                m_row['interest30'] = int(row['audience_interest_apps_30']) if not math.isnan(row['audience_interest_apps_30'])  else 0
            except Exception:
                m_row['interest30'] = 0

            try:
                m_row['interest31'] = int(row['audience_interest_apps_31']) if not math.isnan(row['audience_interest_apps_31'])  else 0
            except Exception:
                m_row['interest31'] = 0

            try:
                m_row['interest32'] = int(row['audience_interest_apps_32']) if not math.isnan(row['audience_interest_apps_32'])  else 0
            except Exception:
                m_row['interest32'] = 0

            try:
                m_row['interest33'] = int(row['audience_interest_apps_33']) if not math.isnan(row['audience_interest_apps_33'])  else 0
            except Exception:
                m_row['interest33'] = 0

            try:
                m_row['interest34'] = int(row['audience_interest_apps_34']) if not math.isnan(row['audience_interest_apps_34'])  else 0
            except Exception:
                m_row['interest34'] = 0

            try:
                m_row['interest35'] = int(row['audience_interest_apps_35']) if not math.isnan(row['audience_interest_apps_35'])  else 0
            except Exception:
                m_row['interest35'] = 0

            try:
                m_row['interest36'] = int(row['audience_interest_apps_36']) if not math.isnan(row['audience_interest_apps_36'])  else 0
            except Exception:
                m_row['interest36'] = 0

            try:
                m_row['interest37'] = int(row['audience_interest_apps_37']) if not math.isnan(row['audience_interest_apps_37'])  else 0
            except Exception:
                m_row['interest37'] = 0

            try:
                m_row['interest38'] = int(row['audience_interest_apps_38']) if not math.isnan(row['audience_interest_apps_38'])  else 0
            except Exception:
                m_row['interest38'] = 0

            try:
                m_row['interest39'] = int(row['audience_interest_apps_39']) if not math.isnan(row['audience_interest_apps_39'])  else 0
            except Exception:
                m_row['interest39'] = 0

            try:
                m_row['interest40'] = int(row['audience_interest_apps_40']) if not math.isnan(row['audience_interest_apps_40'])  else 0
            except Exception:
                m_row['interest40'] = 0

            try:
                m_row['interest41'] = int(row['audience_interest_apps_41']) if not math.isnan(row['audience_interest_apps_41'])  else 0
            except Exception:
                m_row['interest41'] = 0

            modified_df.append(m_row)

    return modified_df


def write_csv(data, filename, delimiter=',', quotechar='"'):
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=cols)
        writer.writerows(data)

    return filename

for filename in sys.argv[1:]:
    tic = time.time()
    modified_df = tf_redshift_format(filename)
    write_csv(modified_df, 'modified_{}'.format(filename))
    toc = time.time()

    print """
    ==============================================
    {}
    tictoc: {}
    ==============================================
    """.format(filename, str(toc-tic))
