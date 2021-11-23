import requests
import pandas as pd

county_df = helper.county_list(key='WaPo')
ohio_county_df = county_df[county_df.BUYER_STATE == 'OH']

df_list = list()
for county in ohio_county_df.BUYER_COUNTY:
    df = helper.county_raw(county=county, state='OH',key='WaPo')
    df_list.append(df)

county_raw = pd.concat(df_list)
county_raw.to_csv('county_raw.tsv.gz', sep = '\t', index = False, compression = "gzip")
