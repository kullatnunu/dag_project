import pandas as pd
import happybase
import numpy as np
from datetime import timedelta, datetime
from translate.settings.logging import Logger
from mongoengine import connect
from mongoengine.queryset.visitor import Q
from mongodb import textHistory
from google.oauth2 import service_account
from translate.settings.defaults import GCP_AUTH
logger = Logger().get()
connect(host="mongodb://localhost:27017/mongo")


def upload_to_gcp(df, gcp_destination_table, project_id, if_exists):
    credentials = service_account.Credentials.from_service_account_info(GCP_AUTH)
    df.to_gbq(gcp_destination_table, project_id, credentials=credentials, if_exists=if_exists)


def ads_report(datetime):
    def group_process(data):
        return_dict = {}
        return_dict['a'] = data[~data['b'].isna()].shape[0]
        return_dict['c'] = data[~data['b'].isna()]['c'].nunique()
        return_dict['d'] = data['b'].isna().sum()

    connection = happybase.Connection('123.456.789.000', 9999)
    table = connection.table('table')
    hbase_key = ['h', 'i', 'j', 'k', 'l', 'm', 'n']
    day = f"_{datetime:%Y-%m-%dT%H}"
    logger.info(f"download start_datetime = {day}")
    day_list = [pre + day for pre in hbase_key]
    data_list = []
    for index, prefix in enumerate(day_list):
        logger.info(f"scan prefix: {prefix}")
        scanner = table.scan(row_prefix=prefix.encode(), batch_size=100000,
                             columns=['a', 'b', 'c', 'd', 'e', 'f', 'g' ])
        for key, value in scanner:
            decode_value = {key.decode().split(':')[1]: val.decode() for key, val in value.items()}
            decode_value['a'] = decode_value['c'][:10]
            decode_value['h'] = decode_value['c'][11:13]
            del decode_value['c']

            try:
                k_cat = textHistory.objects.get(text=decode_value['a'])
                category = k_cat['c'][0]
                decode_value['c'] = category
            except Exception as e:
                pass

            data_list.append(decode_value)
        logger.info(f"{prefix} has {len(data_list)} recoreds")

    data_df = pd.DataFrame.from_dict(data_list)
    report = data_df.groupby(['d', 'h', 'c', 'a', 'r']).apply(group_process).reset_index()
    report = report.sort_values(by=['s'], ascending=False)

    '''update to gcp'''
    gcp_destination_table = 'dashboard_table'
    project_id = 'data-id'
    if_exist = "append"
    upload_to_gcp(report, gcp_destination_table, project_id, if_exist)


def main():
    date = "2019-07-03"
    ads_report(date)


if __name__ == "__main__":
    main()
