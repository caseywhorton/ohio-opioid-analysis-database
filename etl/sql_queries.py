# sql_queries.py

# drop table statements
##############################################
drop_tables = """
drop table if exists county_raw CASCADE;
drop table if exists committee CASCADE;
drop table if exists crp_industry_codes CASCADE;
drop table if exists crp_member CASCADE;
drop table if exists expenditure_codes CASCADE;
drop table if exists candIndbyInd CASCADE;
drop table if exists candcontrib CASCADE;
drop table if exists candsummary CASCADE;
drop table if exists candidate CASCADE;
drop table if exists buyer_address CASCADE;
drop table if exists reporter_address CASCADE;
drop table if exists pharm_location CASCADE;
drop table if exists county_pop CASCADE;
drop table if exists ohio_county CASCADE;
drop table if exists unemployment_rate CASCADE;
drop table if exists overdose CASCADE;
drop table if exists ohio_congress_county CASCADE;
drop table if exists congress_years CASCADE;
drop view if exists drug_counts;
drop view if exists district_quantity_vw;
drop view if exists district_industry;
drop view if exists district_population_vw;
drop view if exists countyrep_vw;
drop view if exists overdose_yr_vw;
drop view if exists candidate_countyrunfor_vw;
"""

# queries
##############################################
ohio_candidate_query = """
select 
    distinct(m.cid),
    y.startyear
from crp_member as m
join congress_years as y
on m.congress = y.congress
where left(m.office,2) = 'OH';
"""

# create table statements
##############################################

create_table_congress_years = """
create table congress_years
(
  congress int not null,
  cycle int not null,
  startyear int not null,
  endyear int not null
)
"""

create_table_ohio_congress_county = """
create table if not exists ohio_congress_county
(
    record_no int,
    distid char(5),
    county varchar,
    startyear int,
    endyear int
)
"""

create_table_overdose = """
create table if not exists overdose
(
county varchar,
state char(2),
county_code decimal,
year decimal,
year_code decimal,
month varchar,
month_code varchar,
drug_alcohol_induced_cause varchar,
drug_alcohol_induced_cause_code varchar,
deaths decimal,
population varchar,
crude_rate varchar
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
    cid_cycle varchar NOT NULL,
    CID varchar NOT NULL,
    CRPName varchar,
    Party varchar,
    DistIDRunFor varchar,
    FECCandID varchar,
    metadata_sheet varchar,
    cycle int,
    PRIMARY KEY (cid_cycle)
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
    metadata_sheet varchar,
    PRIMARY KEY (Catcode)
);
"""

create_table_crp_member = """
create table if not exists crp_member
(
    cid_congress varchar NOT NULL,
    CID varchar NOT NULL,
    CRPName varchar,
    Party varchar,
    Office varchar,
    FECCandID varchar,
    metadata_sheet varchar,
    congress int,
    PRIMARY KEY (cid_congress)
);
"""

create_table_committee = """
create table if not exists committee
(
    CODE varchar,
    CmteName varchar,
    metadata_sheet varchar,
    PRIMARY KEY (CODE)
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

create_table_candcontrib = """
create table if not exists candcontrib
(
    cid_cycle varchar NOT NULL,
    cid varchar NOT NULL,
    cycle int,
    origin varchar,
    source varchar,
    notice varchar,
    org_name varchar,
    total int,
    pacs int,
    indivs int,
    CONSTRAINT fk_candcontrib FOREIGN KEY (cid_cycle) REFERENCES candidate(cid_cycle)
)
"""

create_table_candIndByInd = """
create table if not exists candIndbyInd
(
    cid_cycle varchar NOT NULL,
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
    last_updated date,
    CONSTRAINT fk_candindbyind FOREIGN KEY (cid_cycle) REFERENCES candidate(cid_cycle)
)
"""

create_table_candsummary = """
create table if not exists candsummary
(
    cid_cycle varchar NOT NULL,
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
    last_updated date,
    CONSTRAINT fk_candsummary FOREIGN KEY (cid_cycle) REFERENCES candidate(cid_cycle)
)
"""

create_table_unemployment_rate = """
create table if not exists unemployment_rate
(
    series_id varchar NOT NULL,
    area_code char(15),
    measure_code char(2),
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

copy_csv_data = """
copy {}
from '{}'
iam_role '{}'
format as csv
delimiter ','
IGNOREHEADER 1
EMPTYASNULL
region 'us-west-2';
"""
# insert table statements
##############################################
insert_table_candindbyind = """
INSERT INTO candindbyind (cid_cycle, cid, cycle, industry, chamber, party, state,
       total, indivs, pacs, rank, origin, source, last_updated)
VALUES (%s, %s ,%s ,%s,%s,%s,%s,%s,%s ,%s ,%s ,%s , %s, %s)
"""

insert_table_candsummary = """
INSERT INTO candsummary (cid_cycle, cid, cycle, state, party, chamber, first_elected,
       next_election, total, spent, cash_on_hand, debt, origin, source, last_updated)
VALUES (%s, %s ,%s ,%s, %s, %s, %s, %s, %s ,%s ,%s ,%s , %s, %s, %s)
"""

insert_table_candcontrib = """
INSERT INTO candcontrib (cid_cycle, cid, cycle, origin, source, notice, org_name, total, pacs, indivs)
VALUES (%s, %s ,%s , %s, %s, %s, %s, %s, %s, %s)
"""

insert_table_ohio_county = """
INSERT INTO ohio_county (BUYER_COUNTY, BUYER_STATE, countyfips)
VALUES (%s ,%s ,%s)
"""

insert_table_county_pop = """
INSERT INTO county_pop (countyfips, STATE, COUNTY, variable, year, population)
VALUES (%s ,%s ,%s ,%s , %s, %s)
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
INSERT INTO unemployment_rate (series_id, area_code, measure_code, year, period, value, footnotes)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

insert_table_congress_years = """
insert into congress_years VALUES
(106,	1998,	1999,	2000),
(107,	2000,	2001,	2002),
(108,	2002,	2003,	2004),
(109,	2004,	2005,	2006),
(110,	2006,	2007,	2008),
(111,	2008,	2009,	2010),
(112,	2010,	2011,	2012),
(113,	2012,	2013,	2014),
(114,	2014,	2015,	2016),
(115,	2016,	2017,	2018),
(116,	2018,	2019,	2020),
(117,	2020,	2021,	2022)
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
                        create_table_cu_item,
                        create_table_overdose,
                        create_table_ohio_congress_county,
                        create_table_congress_years
                       ]

##############################################
#create_views = 

create_view_drug_counts = """
CREATE OR REPLACE VIEW "public"."drug_counts" AS  SELECT "left"(county_raw.transaction_date::character varying::text, len(county_raw.transaction_date::character varying::text) % 6) AS "month", "left"("right"(county_raw.transaction_date::character varying::text, 6), 2) AS "day", "right"(county_raw.transaction_date::character varying::text, 4) AS "year", county_raw.drug_name, county_raw.product_name, county_raw.dos_str, sum(county_raw.quantity) AS quantity
   FROM county_raw
  GROUP BY "left"(county_raw.transaction_date::character varying::text, len(county_raw.transaction_date::character varying::text) % 6), "left"("right"(county_raw.transaction_date::character varying::text, 6), 2), "right"(county_raw.transaction_date::character varying::text, 4), county_raw.drug_name, county_raw.product_name, county_raw.dos_str;
"""

create_view_district_quantity_vw = """
CREATE OR REPLACE VIEW "public"."district_quantity_vw" AS  SELECT "right"(county_raw.transaction_date::text, 4) AS "year", ohio_congress_county.distid, sum(county_raw.quantity) AS quantity
   FROM county_raw
   JOIN buyer_address ON county_raw.buyer_dea_no::text = buyer_address.buyer_dea_no::text
   JOIN ohio_congress_county ON lower(buyer_address.buyer_county::text) = ohio_congress_county.county::text
  GROUP BY "right"(county_raw.transaction_date::text, 4), ohio_congress_county.distid
  ORDER BY "right"(county_raw.transaction_date::text, 4), ohio_congress_county.distid;
"""

create_view_district_industry = """
CREATE OR REPLACE VIEW "public"."district_industry" AS  SELECT cte."cycle", cte.congress_startyear, cte.cid, cte.distid, c.industry, c.chamber, c.party, c.total, c.indivs, c.pacs
   FROM ( SELECT countyrep_vw."cycle", countyrep_vw.congress_startyear, countyrep_vw.cid, countyrep_vw.distid
           FROM countyrep_vw
          GROUP BY countyrep_vw."cycle", countyrep_vw.congress_startyear, countyrep_vw.cid, countyrep_vw.distid) cte
   JOIN candindbyind c ON cte.cid::text = c.cid::text AND cte."cycle" = c."cycle";
"""

create_view_district_population_vw = """
CREATE OR REPLACE VIEW "public"."district_population_vw" AS  SELECT cte.distid, cte."year", sum(cte.population) AS population
   FROM ( SELECT ohio_congress_county.distid, county_pop."year", ohio_congress_county.startyear, ohio_congress_county.endyear, county_pop.population
           FROM ohio_congress_county
      JOIN ohio_county ON ohio_congress_county.county::text = lower(ohio_county.buyer_county::text)
   JOIN county_pop ON ohio_county.countyfips = county_pop.countyfips AND county_pop."year"::text >= ohio_congress_county.startyear::text AND county_pop."year"::text <= ohio_congress_county.endyear::text) cte
  GROUP BY cte.distid, cte."year"
  ORDER BY cte.distid, cte."year";
"""

create_view_countyrep_vw = """
CREATE OR REPLACE VIEW "public"."countyrep_vw" AS  SELECT c.cid_cycle, c.cid, c."cycle", y.congress, o.distid, o.county, ohio_county.countyfips, y.startyear AS congress_startyear, y.endyear AS congress_endyear, o.startyear AS county_startyear, o.endyear AS county_endyear
   FROM candidate c
   JOIN congress_years y ON c."cycle" = y."cycle"
   JOIN crp_member m ON m.congress = y.congress AND m.cid::text = c.cid::text
   JOIN ohio_congress_county o ON o.distid = c.distidrunfor::character(256)
   JOIN ohio_county ON o.county::text = lower(ohio_county.buyer_county::text)
  WHERE c."cycle" >= o.startyear AND c."cycle" <= o.endyear;
"""

create_view_overdose_yr_vw = """
CREATE OR REPLACE VIEW "public"."overdose_yr_vw" AS  SELECT overdose.county, overdose."year", sum(overdose.deaths) AS sum
   FROM overdose
  GROUP BY overdose.county, overdose."year";
"""

create_view_candidate_countyrunfor_vw = """
CREATE OR REPLACE VIEW "public"."candidate_countyrunfor_vw" AS  SELECT c.cid_cycle, o.county AS countyrunfor
   FROM candidate c
   LEFT JOIN ohio_congress_county o ON c.distidrunfor::character(256) = o.distid
  WHERE c."cycle" >= o.startyear AND c."cycle" <= o.endyear;
"""

create_view_queries = [create_view_countyrep_vw,
                       create_view_drug_counts,
                       create_view_district_quantity_vw,
                       create_view_district_industry,
                       create_view_district_population_vw,
                       create_view_overdose_yr_vw,
                       create_view_candidate_countyrunfor_vw
                      ]
                       

##############################################
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
