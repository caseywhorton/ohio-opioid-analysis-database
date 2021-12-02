import requests
import pandas as pd
import io
import json

def copy_crp_tables_to_redshift(client, conn, bucket, iam_role):
    """
    copies all crp fact and dimensional files from s3 to redshift tables
    """
    clientResponse = client.list_objects(Bucket = bucket)
    cur = conn.cursor()
    for key in clientResponse['Contents']:

        if key['Key'].find('Candidate') > -1:
            (table, path) = ('candidate','s3://{}/{}'.format(bucket, key['Key']))
        elif key['Key'].find('Member') > -1:
            (table, path) = ('crp_member','s3://{}/{}'.format(bucket, key['Key']))
        elif key['Key'].find('Cmte') > -1:
            (table, path) = ('committee','s3://{}/{}'.format(bucket, key['Key']))
        elif key['Key'].find('Expenditure') > -1:
            (table, path) = ('expenditure_codes','s3://{}/{}'.format(bucket, key['Key']))
        elif key['Key'].find('CRP') > -1:
            (table, path) = ('crp_industry_codes','s3://{}/{}'.format(bucket, key['Key']))
        else:
            continue

        query = """
        copy {}
        from '{}'
        iam_role '{}'
        format as csv
        ignoreheader 1
        EMPTYASNULL
        region 'us-west-2';
        """.format(table, path, iam_role)

        cur.execute(query)
        conn.commit()


def opensecrets_candindbyind(cid, cycle, ind_code, api_key):
    url = "https://www.opensecrets.org/api/?method=candIndByInd&cid={}&cycle={}&ind={}&apikey={}&output=json".format(cid, cycle, ind_code,api_key)
    try:
        re = requests.get(url)
        df_json = re.json()
        x = df_json['response']['candIndus']['@attributes']
        return(pd.json_normalize(x))
    except:
        pass
  

def opensecrets_candsummary(cid, cycle, api_key):
    url = "https://www.opensecrets.org/api/?method=candSummary&cid={}&cycle={}&apikey={}&output=json".format(cid, cycle,api_key)
    try:
        re = requests.get(url)
        df_json = re.json()
        x = df_json['response']['summary']['@attributes']
        return(pd.json_normalize(x))
    except:
        pass
    
    
def opensecrets_candcontrib(cid, cycle,api_key):
    url = "https://www.opensecrets.org/api/?method=candContrib&cid={}&cycle={}&apikey={}&output=json".format(cid, cycle,api_key)
    try:
        re = requests.get(url)
        df_json = re.json()

        left_df = pd.json_normalize(df_json['response']['contributors']['@attributes'])
        left_df_cols = left_df.columns
        
        right_df = pd.json_normalize(df_json['response']['contributors']['contributor'])
        right_df.columns = [x[x.find('.')+1:] for x in right_df.columns]
        x = left_df[left_df_cols].join(right_df, how = 'right')
        
        x[left_df_cols] = x[left_df_cols].ffill()
        
        return(x)
    except:
        pass
   

def county_list(key=''):
    """Get dataframe of counties, states, and fips codes that are represented in the ARCOS data
    Args:
        key: Key needed to make query successful (NOTE: only necessary arg)
    Returns:
        pandas data frame
    """

    if not key:
        print("Please supply a valid API key.")

    url = "https://arcos-api.ext.nile.works/v1/county_list"

    params = {"key": key}
    requestdata = requests.get(url, params=params)
    requestdata = requestdata.json()

    return pd.DataFrame.from_records(requestdata)


def county_raw(county='', state='', key=''):
    """Data from from non-contiguous states not yet processed and available.
    Args:
        county: Filter the data to only this county (e.g. 'Mingo')
        state: Filter the data to county within this state (e.g. 'WV')
        key: Key needed to make query successful
    Returns:
        pandas data frame
    """

    if not key:
        print("Please supply a valid API key.")
    elif not len(state) == 2:
        print("Please supply the U.S. state (in abbreviated form) of interest.")

    url = "https://arcos-api.ext.nile.works/v1/county_data"

    params = {"county": county,
              "state": state,
              "key": key}
    requestdata = requests.get(url, params=params)

    # New transformation of delivery
    return pd.read_csv(io.StringIO(requestdata.text), sep='\t')


def county_population(county='', state='', key=''):
    """Get annual population for counties between 2006 and 2014
    Args:
        county: Filter the data to only this county (e.g. 'Mingo')
        state: Filter the data to county within this state (e.g. 'WV')
        key: Key needed to make query successful
    Returns:
        pandas data frame
    """

    if not key:
        print("Please supply a valid API key.")
    elif not len(state) == 2:
        print("Please supply the U.S. state (in abbreviated form) of interest.")

    url = "https://arcos-api.ext.nile.works/v1/county_population"

    params = {"county": county,
              "state": state,
              "key": key}
    requestdata = requests.get(url, params=params)
    requestdata = requestdata.json()

    return pd.DataFrame.from_records(requestdata)


def buyer_addresses(county='', state='', key=''):
    """Get DEA designated addresses for each pharmacy
    based on BUYER_DEA_NO (Only includes retail and chain pharmacy designations)
    Args:
        county: Filter the data to only this county (e.g. 'Mingo')
        state: Filter the data to county within this state (e.g. 'WV')
        key: Key needed to make query successful
    Returns:
        pandas data frame
    """

    if not key:
        print("Please supply a valid API key.")
    elif not len(state) == 2:
        print("Please supply the U.S. state (in abbreviated form) of interest.")

    url = "https://arcos-api.ext.nile.works/v1/buyer_details"

    params = {"county": county,
              "state": state,
              "key": key}
    requestdata = requests.get(url, params=params)
    requestdata = requestdata.json()

    return pd.DataFrame.from_records(requestdata)


def drug_list(key=''):
    """Get list of drugs available in the ARCOS database
    Args:
        key: Key needed to make query successful
    Returns:
        pandas data frame
    """

    if not key:

        print("Please supply a valid API key.")

    url = "https://arcos-api.ext.nile.works/v1/drug_list"

    params = {"key": key}
    requestdata = requests.get(url, params=params)
    requestdata = requestdata.json()

    return pd.DataFrame.from_records(requestdata)


def pharm_counties(county='', state='', key=''):
    """Get county GEOID for each pharmacy based on
    BUYER_DEA_NO (Only includes retail and chain pharmacy designations)
    Args:
        county: Filter the data to only this county (e.g. 'Mingo')
        state: Filter the data to county within this state (e.g. 'WV')
        key: Key needed to make query successful
    Returns:
        pandas data frame
    """

    if not key:
        print("Please supply a valid API key.")
    elif not len(state) == 2:
        print("Please supply the U.S. state (in abbreviated form) of interest.")

    url = "https://arcos-api.ext.nile.works/v1/pharmacy_counties"

    params = {"county": county,
              "state": state,
              "key": key}
    requestdata = requests.get(url, params=params)
    requestdata = requestdata.json()

    return pd.DataFrame.from_records(requestdata)


def pharm_latlon(county='', state='', key=''):
    """Get latitude and longitude data for each pharmacy based on
    BUYER_DEA_NO (Only includes retail and chain pharmacy designations)
    Args:
        county: Filter the data to only this county (e.g. 'Mingo')
        state: Filter the data to county within this state (e.g. 'WV')
        key: Key needed to make query successful
    Returns:
        pandas data frame
    """

    if not key:
        print("Please supply a valid API key.")
    elif not len(state) == 2:
        print("Please supply the U.S. state (in abbreviated form) of interest.")

    url = "https://arcos-api.ext.nile.works/v1/pharmacy_latlon"

    params = {"county": county,
              "state": state,
              "key": key}
    requestdata = requests.get(url, params=params)
    requestdata = requestdata.json()

    return pd.DataFrame.from_records(requestdata)


def reporter_addresses(county='', state='', key=''):
    """Get DEA designated addresses for each Reporter based
    on REPORTER_DEA_NO (Includes Manufacturers and Distributors)
    Args:
        county: Filter the data to only this county (e.g. 'Mingo')
        state: Filter the data to county within this state (e.g. 'WV')
        key: Key needed to make query successful
    Returns:
        pandas data frame
    """

    if not key:
        print("Please supply a valid API key.")
    elif not len(state) == 2:
        print("Please supply the U.S. state (in abbreviated form) of interest.")

    url = "https://arcos-api.ext.nile.works/v1/reporter_details"

    params = {"county": county,
              "state": state,
              "key": key}
    requestdata = requests.get(url, params=params)
    requestdata = requestdata.json()

    return pd.DataFrame.from_records(requestdata)


def return_ohio_series_list():
    ohio_area_codes = list()

    for i in range(39001, 39177, 2):
        ohio_area_codes.append("CN" + str(i) + str('00000000'))

    series_list = list()

    for measure in ['03','08']:
        for area_code in ohio_area_codes:
            mystring = "LAU" + area_code + measure
            series_list.append(mystring)
    
    return(series_list)


def get_bls_data(seriesid, startyear, endyear):
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": seriesid,"startyear":startyear, "endyear":endyear})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    json_data = json.loads(p.text)
    pd_list = list()
    col_list = ["series id","year","period","value","footnotes"]
    for series in json_data['Results']['series']:
        x=pd.DataFrame(columns = col_list)
        seriesId = series['seriesID']
        for item in series['data']:
            year = item['year']
            period = item['period']
            value = item['value']
            footnotes=""
            for footnote in item['footnotes']:
                if footnote:
                    footnotes = footnotes + footnote['text'] + ','
            if 'M01' <= period <= 'M12':
                pd_list.append((seriesId,year,period,value,footnotes[0:-1]))
    return(pd.DataFrame(pd_list, columns = col_list))


def check_greater_than_zero(table_name, cur, conn):
    cur.execute("SELECT COUNT(*) FROM {}".format(table_name), conn)
    obs_num_rows = cur.fetchone()[0]
    assert obs_num_rows > 0, "0 rows."
    print("{}: Check passed, rows returned: ".format(table_name) + str(obs_num_rows))
    
    
def check_expected_rows(table_name, cur, conn, exp_num_rows):
    cur.execute("SELECT COUNT(*) FROM {}".format(table_name), conn)
    obs_num_rows = cur.fetchone()[0]
    assert obs_num_rows == exp_num_rows, "Mismatched row count. Expected {}, observed {}.".format(exp_num_rows, obs_num_rows)
    print('Checks passed, row counts match.')
    

def clean_overdose_data_file(filepath):
    df = pd.read_csv(filepath, sep = '\t')
    df.drop('Notes', axis=1, inplace=True)
    df.dropna(axis=0, how = 'all', thresh=None, subset=None, inplace=True)
    df.columns = [x.replace(' ','_').replace('/','_') for x in df.columns]
    return(df)
