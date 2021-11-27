# sql_queries.py

# drop table statements
##############################################
drop_tables = """
drop table if exists county_raw;
drop table if exists candidate;
drop table if exists committee;
drop table if exists crp_industry_codes;
drop table if exists crp_member;
drop table if exists expenditure_codes;
drop table if exists candIndbyInd;
drop table if exists candcontrib;
drop table if exists candsummary;
drop table if exists buyer_address;
drop table if exists reporter_address;
drop table if exists county_raw;
drop table if exists drug_list;
drop table if exists pharm_location;
drop table if exists county_pop;
drop table if exists ohio_county;
drop table if exists unemployment_rate;
"""

# queries
##############################################
ohio_candidate_query = """
select 
    distinct(cid),
    right(metadata_sheet,4)
from candidate
where left(distidrunfor,2) = 'OH' and right(metadata_sheet,4) between 2006 and 2014
;
"""

# create table statements
##############################################
create_table_candcontrib = """
create table if not exists candcontrib
(
    cid varchar NOT NULL,
    cycle int,
    origin varchar,
    source varchar,
    notice varchar,
    org_name varchar,
    total int,
    pacs int,
    indivs int
)
"""

create_table_buyer_address = """
create table if not exists buyer_address
(
    BUYER_DEA_NO varchar NOT NULL,
    BUYER_BUS_ACT varchar,
    BUYER_NAME varchar,
    BUYER_ADDRESS1 varchar,
    BUYER_ADDRESS2 varchar,
    BUYER_CITY varchar,
    BUYER_STATE varchar,
    BUYER_ZIP int,
    BUYER_COUNTY varchar,
    BUYER_ADDL_CO_INFO varchar,
    PRIMARY KEY(BUYER_DEA_NO)
)
"""

create_table_reporter_address = """
create table if not exists reporter_address
(
    Reporter_family varchar,
    REPORTER_DEA_NO varchar NOT NULL,
    REPORTER_BUS_ACT varchar,
    REPORTER_NAME varchar,
    REPORTER_ADDRESS1 varchar,
    REPORTER_CITY varchar,
    REPORTER_STATE char(2),
    REPORTER_ZIP int,
    REPORTER_COUNTY varchar,
    PRIMARY KEY(REPORTER_DEA_NO)
)
"""

create_table_county_raw = """
create table if not exists county_raw
(
    REPORTER_DEA_NO varchar NOT NULL,
    BUYER_DEA_NO varchar NOT NULL,
    TRANSACTION_CODE char(2),
    DRUG_CODE decimal,
    NDC_NO varchar,
    DRUG_NAME varchar,
    QUANTITY decimal,
    UNIT char(1),
    ACTION_INDICATOR char(1),
    ORDER_FORM_NO varchar,
    CORRECTION_NO decimal,
    STRENGTH decimal,
    TRANSACTION_DATE decimal,
    CALC_BASE_WT_IN_GM decimal,
    DOSAGE_UNIT decimal,
    TRANSACTION_ID decimal,
    Product_Name varchar,
    Ingredient_Name varchar,
    Measure varchar,
    MME_Conversion_Factor decimal,
    Combined_Labeler_Name varchar,
    Revised_Company_Name varchar,
    Reporter_family varchar,
    dos_str decimal,
    CONSTRAINT fk_reporter_dea_no FOREIGN KEY(REPORTER_DEA_NO) REFERENCES reporter_address(REPORTER_DEA_NO),
    CONSTRAINT fk_buyer_dea_no FOREIGN KEY(BUYER_DEA_NO) REFERENCES buyer_address(BUYER_DEA_NO)
)
"""

create_table_drug_list = """
create table if not exists drug_list
(
    DRUG_NAME varchar not null
)
"""

create_table_pharm_location = """
create table if not exists pharm_location
(
    BUYER_DEA_NO varchar NOT NULL,
    lat decimal,
    lon decimal,
    PRIMARY KEY(BUYER_DEA_NO)
)
"""

create_table_ohio_county = """
create table if not exists ohio_county
(
    BUYER_COUNTY varchar,
    BUYER_STATE char(2),
    countyfips int,
    PRIMARY KEY (countyfips)
)
"""

create_table_county_pop = """
create table if not exists county_pop
(
    countyfips int,
    STATE int,
    COUNTY int,
    variable varchar,
    year varchar,
    population int,
    CONSTRAINT fk_countyfips FOREIGN KEY (countyfips) REFERENCES ohio_county(countyfips)
);
"""

create_table_candidate = """
create table if not exists candidate
(
    CID varchar NOT NULL,
    CRPName varchar,
    Party varchar,
    DistIDRunFor varchar,
    FECCandID varchar,
    metadata_sheet varchar
);
"""

create_table_crp_industry_codes = """
create table if not exists crp_industry_codes
(
    Catcode varchar,
    Catname varchar,
    Carorder varchar,
    Industry varchar,
    Sector varchar,
    SectorLong varchar,
    metadata_sheet varchar
);
"""

create_table_crp_member = """
create table if not exists crp_member
(
    CID varchar NOT NULL,
    CRPName varchar,
    Party varchar,
    Office varchar,
    FECCandID varchar,
    metadata_sheet varchar
);
"""

create_table_committee = """
create table if not exists committee
(
    CODE varchar,
    CmteName varchar,
    metadata_sheet varchar
);
"""

create_table_expenditure_codes = """
create table if not exists expenditure_codes
(
    ExpCode varchar,
    DescripShort varchar,
    DescripLong varchar,
    Sector varchar,
    SectorName varchar,
    metadata_sheet varchar
);
"""

create_table_candIndByInd = """
create table if not exists candIndbyInd
(
    cid varchar NOT NULL,
    cycle int,
    industry varchar,
    chamber varchar,
    party varchar,
    state varchar,
    total int,
    indivs int,
    pacs int,
    rank int,
    origin varchar,
    source varchar, 
    last_updated date
)
"""

create_table_candsummary = """
create table if not exists candsummary
(
    cid varchar NOT NULL,
    cycle varchar,
    state varchar,
    party varchar,
    chamber varchar,
    first_elected int,
    next_election int,
    total decimal,
    spent decimal,
    cash_on_hand decimal,
    debt decimal,
    origin varchar,
    source varchar,
    last_updated date
)
"""

create_table_unemployment_rate = """
create table if not exists unemployment_rate
(
    series_id varchar NOT NULL,
    year int,
    period char(3),
    value decimal,
    footnotes varchar
)
"""

create_table_cw_area = """
create table if not exists cw_area
(
   area_code varchar,
   area_name varchar,
   display_level varchar,
   selectable varchar,
   sort_sequence int,
   PRIMARY KEY (area_code)
)
"""

create_table_cu_item = """
create table if not exists cu_item
(
   item_code varchar,
   item_name varchar,
   display_level varchar,
   selectable varchar,
   sort_sequence int,
   PRIMARY KEY (item_code)
)
"""

create_table_la_area = """
create table if not exists la_area
(
   area_type_code char(1),
   area_code varchar,
   area_text varchar,
   display_level varchar,
   selectable varchar,
   sort_sequence int,
   PRIMARY KEY (area_code),
   CONSTRAINT fk_la_area_type FOREIGN KEY (area_type_code) REFERENCES la_area_type(area_type_code)
)
"""

create_table_la_area_type = """
create table if not exists la_area_type
(
   area_type_code char(1),
   areatype_text varchar,
   PRIMARY KEY (area_type_code)
)
"""

create_table_la_measure = """
create table if not exists la_measure
(
   measure_code char(2),
   measure_text varchar,
   PRIMARY KEY (measure_code)
)
"""

# copy table queries
##############################################
copy_county_raw = """
copy county_raw
from '{}'
iam_role '{}'
delimiter '\t' gzip
IGNOREHEADER 1
EMPTYASNULL
region 'us-west-2';
"""

copy_bls_data = """
copy {}
from '{}'
iam_role '{}'
delimiter '\t'
IGNOREHEADER 1
EMPTYASNULL
region 'us-west-2';
"""

# insert table statements
##############################################
insert_table_candindbyind = """
INSERT INTO candindbyind (cid, cycle, industry, chamber, party, state,
       total, indivs, pacs, rank, origin, source, last_updated)
VALUES (%s ,%s ,%s,%s,%s,%s,%s,%s ,%s ,%s ,%s , %s, %s)
"""

insert_table_candsummary = """
INSERT INTO candsummary (cid, cycle, state, party, chamber, first_elected,
       next_election, total, spent, cash_on_hand, debt, origin, source, last_updated)
VALUES (%s ,%s ,%s, %s, %s, %s, %s, %s ,%s ,%s ,%s , %s, %s, %s)
"""

insert_table_candcontrib = """
INSERT INTO candcontrib (cid, cycle, origin, source, notice, org_name, total, pacs, indivs)
VALUES (%s ,%s , %s, %s, %s, %s, %s, %s, %s)
"""

insert_table_ohio_county = """
INSERT INTO ohio_county (BUYER_COUNTY, BUYER_STATE, countyfips)
VALUES (%s ,%s ,%s)
"""

insert_table_county_pop = """
INSERT INTO county_pop (countyfips, STATE, COUNTY, variable, year, population)
VALUES (%s ,%s ,%s ,%s , %s, %s)
"""

insert_table_drug_list = """
INSERT INTO drug_list (DRUG_NAME)
VALUES (%s)
"""

insert_table_pharm_location = """
INSERT INTO pharm_location (BUYER_DEA_NO, lat, lon)
VALUES (%s, %s, %s)
"""

insert_table_buyer_address = """
INSERT INTO buyer_address (BUYER_DEA_NO,
    BUYER_BUS_ACT,
    BUYER_NAME,
    BUYER_ADDRESS1,
    BUYER_ADDRESS2,
    BUYER_CITY,
    BUYER_STATE,
    BUYER_ZIP,
    BUYER_COUNTY,
    BUYER_ADDL_CO_INFO)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

insert_table_reporter_address = """
INSERT INTO reporter_address (Reporter_family,
    REPORTER_DEA_NO,
    REPORTER_BUS_ACT,
    REPORTER_NAME,
    REPORTER_ADDRESS1,
    REPORTER_CITY,
    REPORTER_STATE,
    REPORTER_ZIP,
    REPORTER_COUNTY)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

insert_table_unemployment_rate = """
INSERT INTO unemployment_rate (series_id, year, period, value, footnotes)
VALUES (%s, %s, %s, %s, %s)
"""

# column name lists
##############################################
pharm_location_cols = ['BUYER_DEA_NO','lat','lon']

county_pop_cols = ['countyfips', 'STATE', 'COUNTY', 'variable', 'year', 'population']

buyer_cols = ['BUYER_DEA_NO',
              'BUYER_BUS_ACT',
              'BUYER_NAME',
              'BUYER_ADDRESS1',
              'BUYER_ADDRESS2',
              'BUYER_CITY',
              'BUYER_STATE',
              'BUYER_ZIP',
              'BUYER_COUNTY',
              'BUYER_ADDL_CO_INFO']

reporter_cols = ['Reporter_family',
                 'REPORTER_DEA_NO',
                 'REPORTER_BUS_ACT',
                 'REPORTER_NAME',
                 'REPORTER_ADDRESS1',
                 'REPORTER_CITY',
                 'REPORTER_STATE',
                 'REPORTER_ZIP',
                 'REPORTER_COUNTY']

county_raw_cols = ['REPORTER_DEA_NO',
                   'BUYER_DEA_NO',
                   'TRANSACTION_CODE',
                   'DRUG_CODE',
                   'NDC_NO',
                   'DRUG_NAME',
                   'QUANTITY',
                   'UNIT',
                   'ACTION_INDICATOR',
                   'ORDER_FORM_NO',
                   'CORRECTION_NO',
                   'STRENGTH',
                   'TRANSACTION_DATE',
                   'CALC_BASE_WT_IN_GM',
                   'DOSAGE_UNIT',
                   'TRANSACTION_ID',
                   'Product_Name',
                   'Ingredient_Name',
                   'Measure',
                   'MME_Conversion_Factor',
                   'Combined_Labeler_Name',
                   'Revised_Company_Name',
                   'Reporter_family',
                   'dos_str']

##############################################
create_table_queries = [create_table_expenditure_codes,
                        create_table_committee,
                        create_table_crp_member,
                        create_table_crp_industry_codes,
                        create_table_candidate,
                        create_table_candIndByInd,
                        create_table_candsummary,
                        create_table_candcontrib,
                        create_table_drug_list,
                        create_table_ohio_county,
                        create_table_county_pop,
                        create_table_pharm_location,
                        create_table_buyer_address,
                        create_table_reporter_address,
                        create_table_county_raw,
                        create_table_unemployment_rate,
                        create_table_la_measure,
                        create_table_la_area_type,
                        create_table_la_area,
                        create_table_cw_area,
                        create_table_cu_item
                       ]

# bureau of labor statistics files and table names

bls_file_list = ['la.area.txt',
                 'la.measure.txt',
                 'cu.item.txt',
                 'la.area_type.txt',
                 'cw.area.txt']

bls_table_list = ['la_area',
                  'la_measure',
                  'cu_item',
                  'la_area_type',
                  'cw_area']
