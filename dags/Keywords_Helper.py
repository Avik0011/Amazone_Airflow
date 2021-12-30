import requests
import time
import gzip
from io import BytesIO
import pandas as pd
import json
import csv
from datetime import date
import constants as constants
x=[]

headers = {
    "Authorization": f"Bearer {constants.token}",
    "Amazon-Advertising-API-ClientId": constants.ClientID,
    "Content-Type": "application/json",
    "Amazon-Advertising-API-Scope": constants.scope
}

def keywords_helper():
    print("kkkkkdkdkdkdkdkdkdo00000--------------------")
    main_df = pd.DataFrame()
    date_range = pd.date_range(start = '2021-12-27', end='2021-12-28')
    for i in date_range:
        report_date = i.strftime("%Y%m%d") #'20211210'
        print(report_date, type(report_date))
        # create report
        recordType = "keywords"
        r = requests.post(
            f'{constants.url}/{constants.version}/{constants.advertise}/{recordType}/report',
            json={
            # "campaignType": "sponsoredProducts",
            "stateFilter": "enabled",
            #   "segment": "query",
            "reportDate": report_date,  #YYYYMMDD
            "metrics": ",".join([
                "campaignName",
                "campaignId",
                "adGroupName",
                "adGroupId",
                "keywordId",
                "keywordText",
                "matchType",
                "impressions",
                "clicks",
                "cost",
                "attributedConversions1d",
                "attributedConversions7d",
                "attributedConversions14d",
                "attributedConversions30d",
                "attributedConversions1dSameSKU",
                "attributedConversions7dSameSKU",
                "attributedConversions14dSameSKU",
                "attributedConversions30dSameSKU",
                "attributedUnitsOrdered1d",
                "attributedUnitsOrdered7d",
                "attributedUnitsOrdered14d",
                "attributedUnitsOrdered30d",
                "attributedSales1d",
                "attributedSales7d",
                "attributedSales14d",
                "attributedSales30d",
                "attributedSales1dSameSKU",
                "attributedSales7dSameSKU",
                "attributedSales14dSameSKU",
                "attributedSales30dSameSKU",
                "attributedUnitsOrdered1dSameSKU",
                "attributedUnitsOrdered7dSameSKU",
                "attributedUnitsOrdered14dSameSKU",
                "attributedUnitsOrdered30dSameSKU"
            ]),
            },
            headers=headers
        )
        r = r.json()
        print('report data',r)
        reportId = r["reportId"]

        time.sleep(2)

        if r['status'] == 'IN_PROGRESS':
            r = requests.get(
                f'{constants.url}/{constants.version}/reports/{reportId}',
                headers=headers,
            )
            r = r.json()
            print('location data', r)

            time.sleep(2)


        r = requests.get(
            r["location"],
            headers=headers,
        )

        x = gzip.decompress(r.content)
        y = json.loads(x.decode("utf-8"))
        print(y, type(y))
        temp_df = pd.DataFrame.from_dict(y)
        temp_df['date'] = report_date
        main_df = main_df.append(temp_df, ignore_index = True)

    main_df.shape
    file_name = 'report_keywords-'+str(date.today())+'.csv'
    main_df.to_csv('~/store_files_airflow/'+file_name, index=False)
    keywords_data = main_df['keywordId'].unique()
    # keywords_data.to_csv('~/store_files_airflow/report_keywords_extended.csv', index=False)
    print("kkkkkk   ",keywords_data)
    # file_name = 'report_keywords_extended.json'
    textfile = open('store_files_airflow/report_keywords_extended.txt', "w")
    for element in keywords_data:
        textfile.write(str(element) + "\n")
    textfile.close()

def keywords_extended_helper():
    # file_name = 'report_keywords_extended.json'
    f = open('store_files_airflow/report_keywords_extended.txt')
 
    # returns JSON object as
    # a dictionary
    print("-------------------------",f)
    x = f
    keywords_data = []
    for kw in x:
        r = requests.get(
            f'{constants.url}/{constants.version}/{constants.advertise}/keywords/extended/{kw}',
            headers=headers)
        r = r.json()
        keywords_data.append(r)

    print(keywords_data, len(keywords_data))
    file_name = 'report_keywords_extended-'+str(date.today())+'.json'
    with open('~/store_files_airflow/'+file_name, 'w') as f:
        json.dump(keywords_data, f)
