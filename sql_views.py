create view transaction_vw as
select 
  transaction_id, 
  transaction_date,
  left(transaction_date, length(transaction_date) % 6) as month,
  left(right(transaction_date, 6), 2) as day,
  right(transaction_date, 4) as year,
  county_raw.reporter_dea_no,
  county_raw.buyer_dea_no,
  lower(buyer_address.buyer_county) as buyer_county,
  buyer_address.buyer_zip,
  lower(reporter_address.reporter_county) as reporter_county,
  reporter_address.reporter_zip
from county_raw
left join buyer_address on county_raw.buyer_dea_no = buyer_address.buyer_dea_no
left join reporter_address on county_raw.reporter_dea_no = reporter_address.reporter_dea_no;
