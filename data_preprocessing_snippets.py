import requests
import pandas as pd
import helper

county_df = helper.county_list(key='WaPo')
ohio_county_df = county_df[county_df.BUYER_STATE == 'OH']

df_list = list()
for county in ohio_county_df.BUYER_COUNTY:
    df = helper.county_raw(county=county, state='OH',key='WaPo')
    df_list.append(df)

county_raw = pd.concat(df_list)
county_raw.to_csv('county_raw.tsv.gz', sep = '\t', index = False, compression = "gzip")

# helpful snippet for pushing data from Excel sheets to individual files on a S3 bucket
bucket = '<ENTER BUCKET NAME>'
for sheet in xdf.sheet_names:
    df = pd.read_excel(filepath, sheet_name = sheet)
    df = df.dropna(axis=1, how = 'all')
    df['metadata_sheet'] = sheet
    if sheet.find('Member') > -1:
        i = -5
        df['congress'] = df.metadata_sheet.apply(lambda x: x[i:-2])
        cid_congress = df.CID + df.congress
        df.insert(0, 'cid_congress', cid_congress)
    elif sheet.find('Candidate') > -1:
        i = -4
        df['cycle'] = df.metadata_sheet.apply(lambda x: x[i:])
        cid_cycle = df.CID + df.cycle
        df.insert(0, 'cid_cycle', cid_cycle)  
            
    helper.write_csv_to_s3(df, bucket = bucket, key = sheet + '.csv')
