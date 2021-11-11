import pandas as pd
import boto3

def concat_sheets(filepath, sheet_list):
    df_list = list()
    for sheet in sheet_list:
        df = pd.read_excel(filepath, sheet_name = sheet)
        df['metadata_sheet'] = sheet
        df_list.append(df)
    return(pd.concat(df_list))

# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id = '',
    aws_secret_access_key = '',
    region_name = 'us-west-2'
)

# Create the S3 object
obj = client.get_object(
    Bucket = 'crp-id-fact-tables',
    Key = 'CRP_IDs_Key_Fact_Tables.xlsx'
)
    
# Read data from the S3 object
xdf = pd.ExcelFile(obj['Body'].read())
    
candidate_sheets = [x for x in xdf.sheet_names if x.find('Candidate')>-1]
member_sheets = [x for x in xdf.sheet_names if x.find('Member')>-1]

candidate_df = concat_sheets(filepath = filepath, sheet_list = candidate_sheets)

member_df = concat_sheets(filepath = filepath, sheet_list = member_sheets)

crp_df = pd.read_excel(filepath, sheet_name = 'CRP Industry Codes')
crp_df['metadata_sheet'] = 'CRP Industry Codes'

exp_df = pd.read_excel(filepath, sheet_name = 'Expenditure Codes')
exp_df['metadata_sheet'] = 'Expenditure Codes'

cmte_df = pd.read_excel(filepath, sheet_name = 'Congressional Cmte Codes')
cmte_df['metadata_sheet'] = 'Congressional Cmte Codes'
