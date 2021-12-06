import pandas as pd
import boto3
import psycopg2
import argparse
import helper
import sql_queries
import yaml
from tqdm import tqdm


def main():
    
    parser = argparse.ArgumentParser(description='Description for argument parser')
    parser.add_argument("config", help = "yaml config file for all parameters")
    args = parser.parse_args()
    config_path = args.config
    
    with open(config_path) as file:
        params = yaml.load(file, Loader=yaml.FullLoader)
    
    # create client connection for s3 stuff
    client = boto3.client(
        's3',
        aws_access_key_id = params['aws_access_key_id'],
        aws_secret_access_key = params['aws_secret_access_key'],
        region_name = 'us-west-2'
    )

    # create postgres connection
    print('Connecting to database...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(params['host'], 
                                                                                   params['dbname'],
                                                                                   params['user'],
                                                                                   params['password'],
                                                                                   params['port']))
    cur = conn.cursor()
    
    print('Dropping tables...')
    cur.execute(sql_queries.drop_tables, conn)
    conn.commit()

    print('Creating tables..')    
    for query in sql_queries.create_table_queries:
        cur.execute(query, conn)
        conn.commit()
    
    print('Copying files to redshift...')
    
    # copy the fact and dimension files from openSecrets to tables on Redshift
    helper.copy_crp_tables_to_redshift(client, conn, params['bucket'], params['iam_role'])
 
    # copy the raw county-level data from the arcos dataset to tables on Redshift
    cur.execute(sql_queries.copy_county_raw.format(params['county_raw_s3'], params['iam_role']))
    conn.commit()
    
    # data quality check that table is not empty
    helper.check_greater_than_zero("county_raw", cur, conn)
    
    # copy the bureau of labor statistics data to tables on Redshift
    bls_dict = dict(zip(sql_queries.bls_table_list,sql_queries.bls_file_list))

    for key in bls_dict:
        query = sql_queries.copy_bls_data.format(key,
                                             's3://bureau-labor-statistics-data-bucket/' + bls_dict[key],
                                             params['iam_role'])
        cur.execute(query)
        conn.commit()
        
        # data quality check that table is not empty
        helper.check_greater_than_zero(key, cur, conn)
       
    # copy overdose data
    query = sql_queries.copy_csv_data.format("overdose",
                                     "s3://ohio-opioid-support-bucket/ohio_overdose_1999_2019.csv",
                                     params['iam_role'])
    
    cur.execute(query)
    conn.commit()
    
    # copy ohio congress county
    query = sql_queries.copy_csv_data.format("ohio_congress_county",
                                     "s3://ohio-opioid-support-bucket/ohio_congress_id_county.csv",
                                     params['iam_role'])
    
    cur.execute(query)
    conn.commit()
    
    print('Calling APIs and inserting data into database...')
    
    # Unemployment rate from bureau of labor statistics
    
    series_list = helper.return_ohio_series_list()
    df = helper.get_bls_data(series_list, 2006, 2014)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to unemployment_rate'):
        cur.execute(sql_queries.insert_table_unemployment_rate, list(row))
    
    conn.commit()
 
    # data quality check that table has number of expected rows
    helper.check_expected_rows("unemployment_rate", cur, conn, len(df))
    
    # Ohio candidate query from opensecrets
    
    cur.execute(sql_queries.ohio_candidate_query, conn)
    ohio_candidates = cur.fetchall()
    
    # candindbyind from opensecrets
    
    df_list = list()
    for result in tqdm(ohio_candidates, desc = 'candIndByInd'):
        cid, year = result
        try:
            df = helper.opensecrets_candindbyind(cid,year,'H04', params['opensecrets_api_key'])
            df.drop('cand_name',axis=1,inplace=True)
            cid_cycle = df.cid + df.cycle
            df.insert(0, 'cid_cycle', cid_cycle)
            df_list.append(df)
        except:
            pass
       
    df = pd.concat(df_list)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to candindbyind'):
        cur.execute(sql_queries.insert_table_candindbyind, list(row))
        
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("candindbyind", cur, conn, len(df))
    
    # candsummary from opensecrets
    
    df_list = list()
    for result in tqdm(ohio_candidates, desc = 'candsummary'):
        cid, year = result
        df = helper.opensecrets_candsummary(cid,year, params['opensecrets_api_key'])
        try:
            df = df.replace('',0)
            df.drop('cand_name',axis=1,inplace=True)
            cid_cycle = df.cid + df.cycle
            df.insert(0, 'cid_cycle', cid_cycle)
            df_list.append(df)
        except:
            pass
        
    df = pd.concat(df_list)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to candsummary'):
        cur.execute(sql_queries.insert_table_candsummary, list(row))
     
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("candsummary", cur, conn, len(df))
    
    # candcontrib from opensecrets
    
    df_list = list()
    for result in tqdm(ohio_candidates, desc = 'candcontrib'):
        cid, year = result
        df = helper.opensecrets_candcontrib(cid, year, params['opensecrets_api_key'])
        try:
            df.drop('cand_name',axis=1,inplace=True)
            cid_cycle = df.cid + df.cycle
            df.insert(0, 'cid_cycle', cid_cycle)
            df_list.append(df)
        except:
            pass
       
    df = pd.concat(df_list)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to candcontrib'):
        cur.execute(sql_queries.insert_table_candcontrib, list(row))
    
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("candcontrib", cur, conn, len(df))
    
    # county list of ohio counties from arcos dataset
    
    county_df = helper.county_list(key=params['arcos_api_key'])
    ohio_county_df = county_df[county_df.BUYER_STATE == 'OH']
    
    for i, row in tqdm(ohio_county_df.iterrows(), total = len(ohio_county_df), desc = 'Inserting Rows to ohio_county'):
        cur.execute(sql_queries.insert_table_ohio_county, list(row))
    
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("ohio_county", cur, conn, len(ohio_county_df))
    
    # county population from arcos dataset
    
    df_list = list()
    
    for county in ohio_county_df.BUYER_COUNTY.unique():
        try:
            df = helper.county_population(county=county, state='OH',key=params['arcos_api_key'])
            df_list.append(df[sql_queries.county_pop_cols])
        except:
            pass

    df = pd.concat(df_list)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to county_pop'):
        cur.execute(sql_queries.insert_table_county_pop, list(row))
     
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("county_pop", cur, conn, len(df))
    
    # drug list table from arcos dataset
    
    df = helper.drug_list(key=params['arcos_api_key'])
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to drug_list'):
        cur.execute(sql_queries.insert_table_drug_list, list(row))
       
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("drug_list", cur, conn, len(df))
    
    # pharmacy location data from arcos dataset
    
    df_list = list()

    for county in ohio_county_df.BUYER_COUNTY.unique():
        try:
            df = helper.pharm_latlon(county=county, state='OH',key=params['arcos_api_key'])
            df_list.append(df[sql_queries.pharm_location_cols])
        except:
            pass

    df = pd.concat(df_list)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to pharm_location'):
        cur.execute(sql_queries.insert_table_pharm_location, list(row))
    
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("pharm_location", cur, conn, len(df))
    
    # buyer address from arcos dataset
    
    df_list = list()

    for county in ohio_county_df.BUYER_COUNTY.unique():
        try:
            df = helper.buyer_addresses(county=county, state='OH',key=params['arcos_api_key'])
            df_list.append(df[sql_queries.buyer_cols])
        except:
            pass

    df = pd.concat(df_list)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to buyer_address'):
        cur.execute(sql_queries.insert_table_buyer_address, list(row))
        
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("buyer_address", cur, conn, len(df))
    
    # reporter address from arcos dataset
    
    df_list = list()

    for county in ohio_county_df.BUYER_COUNTY.unique():
        try:
            df = helper.reporter_addresses(county=county, state='OH',key=params['arcos_api_key'])
            df_list.append(df[sql_queries.reporter_cols])
        except:
            pass

    df = pd.concat(df_list)
    
    for i, row in tqdm(df.iterrows(), total = len(df), desc = 'Inserting Rows to reporter_address'):
        cur.execute(sql_queries.insert_table_reporter_address, list(row))
        
    conn.commit()
    
    # data quality check that table has number of expected rows
    helper.check_expected_rows("reporter_address", cur, conn, len(df))
    
if __name__ == "__main__":
    main()
