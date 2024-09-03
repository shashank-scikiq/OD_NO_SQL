with base_table as (select
date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) as "Date",
"network order id",
case
	when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
	and "item consolidated category" is null
	or "item consolidated category" = '' then 'F&B'
	when "seller np name" like '%dominos%'
	and "item consolidated category" is null
	or "item consolidated category" = '' then 'F&B'
	when "item consolidated category" like 'Agri%'
	or "domain" like '%AGR%' then 'Agriculture'
	when "seller np name" like '%agrevolution%' then 'Agriculture'
	when "seller np name" like '%enam.gov%' then 'Agriculture'
	when "seller np name" like '%crofarm%'
	and "item consolidated category" is null
	or "item consolidated category" = '' then 'Grocery'
	when "seller np name" like '%rebelfoods%'
	and "item consolidated category" is null
	or "item consolidated category" = '' then 'F&B'
	when "seller np name" like '%uengage%'
	and "item consolidated category" is null
	or "item consolidated category" = '' then 'F&B'
	when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
	and "item consolidated category" is null
	or "item consolidated category" = '' then 'F&B'
	when "seller np name" = 'api.kiko.live/ondc-seller'
	and "item consolidated category" is null
	or "item consolidated category" = '' then 'Grocery'
	when "item category" = 'F&B' then 'F&B'
	when "item category" = 'Grocery' then 'Grocery'
	when "item category" is not null
	and "item consolidated category" is null then 'Others'
	when "item category" is null then 'Undefined'
	else "item consolidated category"
end as "Order Category",
coalesce("item category",'Undefined') as "item category"
from "default".shared_order_fulfillment_nhm_fields_view_hudi
where
(case 
				when upper(on_confirm_sync_response) = 'NACK' then 1
				when on_confirm_error_code is not null then 1
			else 0
			end) = 0
group by 1,2,3,4)
select *,
case
when row_number() over (partition by "network order id"||'-'||"Order Category"
order by
"network order id") > 1 then 0
else 1
end as "comp_key",
case
when row_number() over (partition by "network order id"||'-'||"item category"
order by
"network order id") > 1 then 0
else 1
end as "comp-subkey",
case
when row_number() over (partition by "network order id"
order by "network order id") > 1 then 0
else 1
end as "noi_key"
from base_table