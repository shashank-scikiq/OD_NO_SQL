with base_table as (
select
	"network order id",
	"buyer np name",
	"seller np name",
	"fulfillment status",
				case
				when "seller pincode" like '%XXX%' then 'Undefined'
		when "seller pincode" like '' then 'Undefined'
		when "seller pincode" like '%*%' then 'Undefined'
		when "seller pincode" like 'null' then 'Undefined'
		when "seller pincode" is null then 'Undefined'
		else "seller pincode"
	end as "Seller Pincode",
			case
				when upper("Delivery Pincode") like '%XXX%' then 'Undefined'
		when "Delivery pincode" like '' then 'Undefined'
		when "Delivery Pincode" like '%*%' then 'Undefined'
		when "Delivery Pincode" like 'null' then 'Undefined'
		when "Delivery Pincode" is null then 'Undefined'
		else "Delivery Pincode"
	end as "Delivery Pincode",
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
	coalesce("item category",
	'Undefined') as "item category",
	date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) as "Date",
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
	"default".shared_order_fulfillment_nhm_fields_view_hudi
where
	(case 
				when upper(on_confirm_sync_response) = 'NACK' then 1
		when on_confirm_error_code is not null then 1
		else 0
	end) = 0),
tail as (
select
	"buyer np name",
	"seller np name",
	"item category",
	"Order Category",
	"network order id",
	"Seller Pincode",
	"Delivery Pincode",
	"Date",
	case
		when "order status" is null
			or "order status" = '' then 'In Process'
			when "order status" = 'Cancelled' then 'Cancelled'
			when "order status" = 'Completed' then 'Delivered'
			when lower("order status") = 'delivered' then 'Delivered'
			when "order status" like 'Liquid%' then 'Delivered'
			when "order status" like '%leted' then 'Delivered'
			when "order status" like 'Return%' then 'Delivered'
			when "fulfillment status" like 'RTO%' then 'Cancelled'
			else 'In Process'
		end as "ONDC order_status",
		case
			when row_number() over (partition by "network order id" || '-' || "Order Category"
		order by
			"network order id") > 1 then 0
			else 1
		end as "comp_key",
		case
			when row_number() over (partition by "network order id" || '-' || "item category"
		order by
			"network order id") > 1 then 0
			else 1
		end as "comp-subkey",
		case
			when row_number() over (partition by "network order id"
		order by
			"network order id") > 1 then 0
			else 1
		end as "noi_key"
	from
		base_table)
select
	"buyer np name",
	"seller np name",
	"item category",
	"Order Category",
	"Seller Pincode",
	"Delivery Pincode",
	"Date",
	"ONDC order_status",
	sum("comp_key") as "comp_key",
	sum("comp-subkey") as "comp-subkey",
	sum("noi_key") as "noi_key"
from
	tail
group by
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8