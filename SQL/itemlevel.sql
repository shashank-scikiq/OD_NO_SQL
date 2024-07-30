with  early_layer as (
select *,
case 
	when substring("f_order delivered at date & time from fulfillments",1,4) = '0000' then 0
	else 1 end as "omitter"
	from "default".shared_order_fulfillment_nhm_fields_view_hudi ), 
base_table as (select 
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
coalesce("item category",'Undefined') as "item category",
SUM(CAST(qty AS decimal)) AS qty,
ARRAY_JOIN(ARRAY_AGG(distinct "on_confirm_sync_response"
order by
	"on_confirm_sync_response"),
	',') as "on_confirm_response",
	ARRAY_JOIN(ARRAY_AGG(distinct "on_confirm_error_code"
order by
	"on_confirm_error_code"),
	',') as "on_confirm_error_code"
from early_layer
where
	date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) >= date('2023-11-01')
	and "network order id" is not null
group by 1,2,3)
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