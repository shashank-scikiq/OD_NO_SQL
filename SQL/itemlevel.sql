with bottom_layer as (
select *,
coalesce ((case
		when "fulfillment status" like '%RTO%' and ("cancellation code" is null or "f_cancellation_code" is null) then '013'
		when "cancellation code" is null then null
	when substring("cancellation code",-3, 4) not in ('001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016',
'017', '018', '019', '020', '021', '022') then '052'
	else substring("cancellation code",-3, 4)
end),
	(case
		when "f_cancellation_code" is null then null
	when substring("f_cancellation_code",-3, 4) not in ('001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016',
'017', '018', '019', '020', '021', '022') then '052'
	else substring("f_cancellation_code",-3, 4)
end),'050') 
as "cancellation_code"
from
	"default".shared_order_fulfillment_nhm_fields_view_hudi
where
	date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) >= date('2023-11-01')
	and "network order id" is not null
	and "network order id" <> ''
	and not (
 lower("buyer np name") like '%stg%'
		or lower("buyer np name") like '%preprod%'
			or lower("buyer np name") like '%pre-prod%'
				or lower("buyer np name") like 'buyer-refapp-ops.ondc.org'
					or lower("buyer np name") like '%staging%'
						or lower("buyer np name") like '%testing%'
							or lower("buyer np name") like '%test%' )
	and not (
lower("seller np name") like '%rapidor%'
		or lower("seller np name") like '%staging%'
			or lower("seller np name") like '%preprod%'
				or lower("seller np name") like '%pre-prod%'
					or lower("seller np name") like 'gl-6912-httpapi.glstaging.in/gl/ondc'
						or lower("seller np name") like '%testing%'
							or lower("seller np name") like '%test%'
								or lower("seller np name") like '%ultron%')),
order_table as (
select
"network order id",
provider_id,
row_number() over (partition by ("network order id" ||
(case
when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%dominos%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%agrevolution%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%enam.gov%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%crofarm%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "seller np name" like '%rebelfoods%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%uengage%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.kiko.live/ondc-seller'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "seller np name" like '%snapdeal%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Fashion'
when "item category" = 'F&B' then 'F&B'
when "item category" = 'Grocery' then 'Grocery'
when trim("item category") is not null
and trim("item consolidated category") is null then 'Others'
when trim("item category") is null then 'Undefined'
else trim("item consolidated category")
end))
order by
(
case
when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%dominos%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%agrevolution%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%enam.gov%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%crofarm%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "seller np name" like '%rebelfoods%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%uengage%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.kiko.live/ondc-seller'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "seller np name" like '%snapdeal%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Fashion'
when "item category" = 'F&B' then 'F&B'
when "item category" = 'Grocery' then 'Grocery'
when trim("item category") is not null
and trim("item consolidated category") is null then 'Others'
when trim("item category") is null then 'Undefined'
else trim("item consolidated category")
end) ) max_record_key,
case
when trim("item category") is null then 'Undefined'
else trim("item category")
end as "item category",
case
when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%dominos%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%agrevolution%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%enam.gov%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%crofarm%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "seller np name" like '%rebelfoods%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%uengage%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.kiko.live/ondc-seller'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "seller np name" like '%snapdeal%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Fashion'
when "item category" = 'F&B' then 'F&B'
when "item category" = 'Grocery' then 'Grocery'
when trim("item category") is not null
and trim("item consolidated category") is null then 'Others'
when trim("item category") is null then 'Undefined'
else trim("item consolidated category")
end as "item consolidated category"
from
bottom_layer),
order_table1 as (
select
"network order id",
case
when ARRAY_JOIN(ARRAY_AGG(distinct "item consolidated category"),
',') like '%F&B%'
and ARRAY_JOIN(ARRAY_AGG(distinct "item consolidated category"),
',') like '%Undefined%' then 'F&B'
when ARRAY_JOIN(ARRAY_AGG(distinct "item consolidated category"),
',') like '%,%' then 'Multi Category'
else ARRAY_JOIN(ARRAY_AGG(distinct "item consolidated category"),
',')
end as "Category"
from
order_table
where
max_record_key = 1
group by
1),
main_table as (
select
"buyer np name" as "Buyer NP",
"seller np name" as "Seller NP",
"domain",
"seller name",
provider_id,
case
when "f_order-picked-up at date & time" = ''
or "f_order-picked-up at date & time" is null then 0
else 1
end as "Shipped at",
case
when upper("seller pincode") like '%XXX%' then 'Undefined'
when upper("seller pincode") like '' then 'Undefined'
when upper("seller pincode") like '%***%' then 'Undefined'
when upper("seller pincode") like 'null' then 'Undefined'
when upper("seller pincode") is null then 'Undefined'
else "seller pincode"
end as "Seller Pincode",
case
when upper("Delivery Pincode") like '%XXX%' then 'Undefined'
when upper("Delivery pincode") like '' then 'Undefined'
when upper("Delivery Pincode") like '%***%' then 'Undefined'
when upper("Delivery Pincode") is null then 'Undefined'
else "Delivery Pincode"
end as "Delivery Pincode",
case
when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%dominos%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%agrevolution%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%enam.gov%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Agriculture'
when "seller np name" like '%crofarm%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "seller np name" like '%rebelfoods%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" like '%uengage%'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'F&B'
when "seller np name" = 'api.kiko.live/ondc-seller'
and trim("item consolidated category") is null
or "item consolidated category" = '' then 'Grocery'
when "item category" = 'F&B' then 'F&B'
when "item category" = 'Grocery' then 'Grocery'
when trim("item category") is not null
and trim("item consolidated category") is null then 'Others'
when trim("item category") is null then 'Undefined'
else trim("item consolidated category")
end as "item consolidated category",
case
when trim("item category") is null
or "item category" = '' then 'Undefined'
else trim("item category")
end as "item category ind",
"network order id" as "Network order id",
case
		when "fulfillment status" like '%RTO%' then coalesce("cancellation_code",'013')
	when (case
			when trim("order status") = 'Cancelled' then 'Cancelled'
			else "fulfillment status" end) = 'Cancelled' then coalesce("cancellation_code",'050')
	else null
end as "cancellation_code",
case
when trim("order status") like '%Return%' then 'Delivered'
when trim("fulfillment status") like 'RTO%' then 'Cancelled'
when ("o_completed on date & time" is not null
or "F_Order Delivered at Date & Time From Fulfillments" is not null)
and "o_cancelled at date & time" is null then 'Delivered'
when "o_completed on date & time" is null
and "F_Order Delivered at Date & Time From Fulfillments" is null
and "o_cancelled at date & time" is not null then 'Cancelled'
when ("o_completed on date & time" is not null
or "F_Order Delivered at Date & Time From Fulfillments" is not null)
and "o_cancelled at date & time" is not null then 'In Process'
when trim(lower("order status")) = 'completed' then 'Delivered'
when trim(lower("order status")) = 'cancelled' then 'Cancelled'
else 'In Process'
end as "order status",
qty,
	ARRAY_JOIN(ARRAY_AGG(distinct "on_confirm_sync_response"
order by
	"on_confirm_sync_response"),
	',') as "on_confirm_response",
	ARRAY_JOIN(ARRAY_AGG(distinct "on_confirm_error_code"
order by
	"on_confirm_error_code"),
	',') as "on_confirm_error_code",
min(DATE(SUBSTRING("O_Created Date & Time", 1, 10))) as "Date"
from
bottom_layer
group by
1,
2,
3,
4,
5,
6,
7,
8,
9,
10,
11,
12,
13,14),
trial as (
select
m.*,
(case
when ot."Category" = 'F&B' then 'F&B'
else m."item consolidated category"
end) as "Order Category",
case
when ((case
when ot."Category" = 'F&B' then 'F&B'
else m."item consolidated category"
end) = 'F&B'
and m."item category ind" = 'Undefined')
or ((case
when ot."Category" = 'F&B' then 'F&B'
else m."item consolidated category"
end) = 'F&B'
and m."item category ind" like '%F&B%') then 'F&B'
else m."item category ind"
end as "item category"
from
main_table m
left join order_table1 ot on
ot."network order id" = m."Network order id"),
final_table as (
select
*,
("Network order id" || '_' || "Order Category") as "comp-key",
("Network order id" || '_' || "item category") as "comp-subkey"
from
trial)
select
"Buyer NP",
"Seller NP",
"Network order id",
provider_id,
"domain",
"seller name",
"Shipped at",
"Seller Pincode",
"Delivery Pincode",
"cancellation_code",
"order status",
"qty",
"Date",
"Order Category",
"item category",
"on_confirm_response",
"on_confirm_error_code",
case
when row_number() over (partition by "Network order id"
order by
"Date") > 1 then 0
else 1
end as "noi_key",
case
when row_number() over (partition by "comp-key"
order by
"Date") > 1 then 0
else 1
end as "comp_key",
case
when row_number() over (partition by "comp-subkey"
order by
"Date") > 1 then 0
else 1
end as "comp-subkey"
from
final_table
-- Filter out the orders with on_confirm_response = 'NACK', we don't consider these orders as confirmed orders