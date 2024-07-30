with bottom_layer as (
select *,
	case
		when "fulfillment status" like '%RTO%' and ("cancellation code" is null or "f_cancellation_code" is null) then '013'
		when "cancellation code" is null then null
	when substring("cancellation code",-3, 4) not in ('001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016',
'017', '018', '019', '020', '021', '022') then '052'
	else substring("cancellation code",-3, 4)
end as "cancel_code",
	case
		when "f_cancellation_code" is null then null
	when substring("f_cancellation_code",-3, 4) not in ('001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016',
'017', '018', '019', '020', '021', '022') then '052'
	else substring("f_cancellation_code",-3, 4)
end as "f_cancel_code"
from
	"default".shared_order_fulfillment_nhm_fields_view_hudi
where
     date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) >= date('2023-11-01')
	and "network order id" is not null
	and 
	(case
		when SUBSTRING("f_order delivered at date & time from fulfillments", 1, 4) = '0000' then 0
		else 1
	end) = 1
),	
base_table as (
select
	"network order id",
	"buyer np name",
	"seller np name",
	"fulfillment status",
	case 
		when "on_confirm_sync_response" is null then 'NULL'
		else "on_confirm_sync_response"
	end as "on_confirm_sync_response",
	case 
		when "on_confirm_error_code" is null then 'NULL'
		else "on_confirm_error_code"
	end as "on_confirm_error_code",
	coalesce("cancel_code","f_cancel_code",'050')as "cancellation_code",
	"f_rto_requested_at",
	row_number() over (partition by ("network order id" ||
		(case
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
end))
order by
		(
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
end) ) max_record_key,
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
end as "item consolidated category",
	"domain",
	date_parse("o_completed on date & time",
		'%Y-%m-%dT%H:%i:%s') as "Completed at",
	date_parse("o_cancelled at date & time",
		'%Y-%m-%dT%H:%i:%s') as "Cancelled at",
	date_parse("f_order-picked-up at date & time",
		'%Y-%m-%dT%H:%i:%s') as "Shipped at",
		date_parse("f_packed at date & time",
		'%Y-%m-%dT%H:%i:%s') as "Ready to Ship",
	date_parse("Promised time to deliver Date & Time from on_select",
		'%Y-%m-%dT%H:%i:%s') as "Promised time",
	"Delivery Pincode",
	date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s') as "Created at",
	date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) as "Date",
	"provider_id",
	"seller pincode",
	"seller name",
	date_parse("F_Order Delivered at Date & Time From Fulfillments",
		'%Y-%m-%dT%H:%i:%s') as "Completed at Ful",
	case
		when not("order status" in ('Cancelled', 'Completed')
		or ("order status" like '%Return%')) then 
	(case
			when ("o_completed on date & time" is not null
			or "F_Order Delivered at Date & Time From Fulfillments" is not null)
			and "o_cancelled at date & time" is null then 'Completed'
			when "o_completed on date & time" is null
			and "F_Order Delivered at Date & Time From Fulfillments" is null
			and "o_cancelled at date & time" is not null then 'Cancelled'
			when ("o_completed on date & time" is not null
			or "F_Order Delivered at Date & Time From Fulfillments" is not null)
			and "o_cancelled at date & time" is not null then "order status"
			else "order status"
		end)
		else "order status"
	end as "order status"
from
	bottom_layer),								
table1 as (
select
	"seller np name" as "Seller NP",
		"buyer np name" as "Buyer NP",
		"max_record_key",
		"domain",
		"Created at",
		"Shipped at",
		"Ready to Ship",
		"on_confirm_sync_response",
		"on_confirm_error_code",
		coalesce(case
		when "item consolidated category" = 'F&B' then "Promised time" + interval '5' minute
		else "Promised time"
	end,
	"Created at") as "Promised time",
		"Date",
		"fulfillment status",
		"item consolidated category",
		"network order id" as "Network order id",
		case
		when trim("order status") is null then 'In-progress'
		when trim("order status") = '' then 'In-progress'
		when trim("order status") like '%**%' then 'In-progress'
		else trim("order status")
	end as "Order Status",
		case
		when trim(lower("order status")) = 'completed' then 'Delivered'
		when trim("fulfillment status") like 'RTO%' then 'Cancelled'
		when trim("order status") like '%Return%' then 'Delivered'
		when trim(lower("order status")) = 'cancelled' then 'Cancelled'
		else 'In Process'
	end as "ONDC order_status",
case
		when "fulfillment status" like '%RTO%' then coalesce("cancellation_code",'013')
	when (case
			when trim("order status") = 'Cancelled' then 'Cancelled'
			else "fulfillment status" end) = 'Cancelled' then coalesce("cancellation_code",'050')
	else null
end as "cancellation_code",
			case
			when (case
			when trim("order status") = 'Completed' then 'Delivered'
			when trim("fulfillment status") like 'RTO%' then 'Cancelled'
			when trim("order status") like '%Return%' then 'Delivered'
			when trim("order status") = 'Cancelled' then 'Cancelled'
			else 'In Process'
		end) = 'Delivered' then coalesce("Completed at Ful",
				"Completed at")
		else null
	end as "Completed at",
			case
				when (case
			when trim("fulfillment status") like 'RTO%' then 'Cancelled'
			when trim(lower("order status")) = 'cancelled' then 'Cancelled'
			else null
		end) = 'Cancelled' then "Cancelled at"
		else null
	end as "Cancelled at",
			provider_id ,
			case
				when upper("seller pincode") like '%XXX%' then 'Undefined'
		when upper("seller pincode") like '' then 'Undefined'
		when upper("seller pincode") like '%*%' then 'Undefined'
		when upper("seller pincode") like 'null' then 'Undefined'
		when upper("seller pincode") is null then 'Undefined'
		else "seller pincode"
	end as "Seller Pincode",
			case
				when upper("Delivery Pincode") like '%XXX%' then 'Undefined'
		when upper("Delivery pincode") like '' then 'Undefined'
		when upper("Delivery Pincode") like '%*%' then 'Undefined'
		when upper("Delivery Pincode") like 'null' then 'Undefined'
		when upper("Delivery Pincode") is null then 'Undefined'
		else "Delivery Pincode"
	end as "Delivery Pincode",
			lower(trim("seller name")) as "seller name"
from
			base_table),
merger_table as (
select
	"Network order id",
	ARRAY_JOIN(ARRAY_AGG(distinct "on_confirm_sync_response"
order by
	"on_confirm_sync_response"),
	',') as "on_confirm_response",
	ARRAY_JOIN(ARRAY_AGG(distinct "on_confirm_error_code"
order by
	"on_confirm_error_code"),
	',') as "on_confirm_error_code",
	ARRAY_JOIN(ARRAY_AGG(distinct "domain"
order by
	"domain"),
	',') as "domain",
	ARRAY_JOIN(ARRAY_AGG(distinct "ONDC order_status"
order by
	"ONDC order_status"),
	',') as "ONDC order_status",
	ARRAY_JOIN(ARRAY_AGG(distinct "item consolidated category"
order by
	"item consolidated category"),
	',') as "item consolidated category"
from
	table1
group by
	1),
trial as 
(
select
	t1."Buyer NP",
	t1."Seller NP",
	t1."Created at",
	t1."Network order id",
	t1."seller name",
	t1."Shipped at",
	t1."Ready to Ship",
	t1."Promised time",
	t1."Date",
	t1."Order Status",
	t1."cancellation_code",
	t1."Completed at",
	t1."Cancelled at",
	t1."provider_id",
	t1."Seller Pincode",
	t1."Delivery Pincode",
	t1."max_record_key",
	mt."on_confirm_response",
	mt."on_confirm_error_code",
	mt."domain",
	mt."ONDC order_status",
	mt."item consolidated category"
from
	table1 t1
join merger_table mt on
	t1."Network order id" = mt."Network order id"),
last_table as (
select
	*
from
	trial
where
	max_record_key = 1),
table_l as (
select
	"Buyer NP",
	"Seller NP",
	"Network order id",
	"on_confirm_response",
	"on_confirm_error_code",
	"domain",
	"item consolidated category" as "Consolidated_category",
	case
		when "ONDC order_status" like '%,%' then 'In Process'
		else "ONDC order_status"
	end as "ONDC order_status",	
	case
		when "item consolidated category" like '%F&B%'
			and "item consolidated category" like '%Undefined%' then 'F&B'
			when "item consolidated category" like '%,%' then 'Multi Category'
			else "item consolidated category"
		end as "Category",
		concat("Seller NP", '_', LOWER("provider_id")) as "provider key",
		ARRAY_JOIN(ARRAY_AGG(distinct "provider_id" order by "provider_id"),',') as "Provider Id",
		ARRAY_JOIN(ARRAY_AGG(distinct "seller name" order by "seller name"),',') as "Seller Name",
		max("Seller Pincode") as "Seller Pincode",
		max("Delivery Pincode") as "Delivery Pincode",
		MIN("cancellation_code") as "cancellation_code",
		max("Created at") as "Created at",
		max("Date") as "Date",
		max("Shipped at") as "Shipped at",
		max("Completed at") as "Completed at",
		max("Cancelled at") as "Cancelled at",
		max("Ready to Ship") as "Ready to Ship",
		max("Promised time") as "Promised time",
		DATE_DIFF('second',
		max("Promised time"),
		max("Completed at")) as "tat_dif",
		DATE_DIFF('day',
		max("Promised time"),
		max("Completed at")) as "tat_diff_days",
		DATE_DIFF('day',
		max("Created at"),
		max("Completed at")) as "day_diff",
		DATE_DIFF('minute',
		max("Created at"),
		max("Completed at")) as "min_diff",
		DATE_DIFF('minute',
		max("Created at"),
		max("Promised time")) as "tat_time",
		case
			when row_number() over (partition by "Network order id"
		order by
				MAX("Date") desc) > 1 then 0
			else 1
		end as "no_key"
	from
		last_table
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
		10)
select
	"Buyer NP",
	"Seller NP",
	"Network order id",
	"provider key",
	"Provider Id",
	"Seller Name",
	"Seller Pincode",
	"Delivery Pincode",
	"cancellation_code",
	"Created at",
	"Date",
	"domain",
	case
		when ("ONDC order_status" = 'Delivered'
			and ("Completed at" <= "Promised time")
				and "Completed at" is not null) then 1
		else 0
	end as "on-time-del",
	"Shipped at",
	"Ready to Ship",
	"Promised time",
	"tat_dif",
	"tat_diff_days",
	"day_diff",
	"min_diff",
	"tat_time",
	"no_key",
	"ONDC order_status",
	case
		when "ONDC order_status" = 'Delivered' then "Completed at"
		when "ONDC order_status" = 'Cancelled' then "Cancelled at"
		else "Created at"
	end as "Updated at",
	"Category",
	"Consolidated_category",
	"on_confirm_response",
	"on_confirm_error_code"
from
	table_l