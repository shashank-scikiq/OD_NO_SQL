with table1 as (
select
	"buyer np name",
	"seller np name",
	"network order id",
	"network transaction id",
	"fulfillment status",
	"Domain",
	DATE(SUBSTRING("O_Created Date & Time",
		1,
		10)) as "Date",
	date_parse("o_completed on date & time",
		'%Y-%m-%dT%H:%i:%s') as "Delivered at",
	date_parse("o_cancelled at date & time",
		'%Y-%m-%dT%H:%i:%s') as "Cancelled at",
	date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s') as "Created at",
	"provider_id",
	"provider_name",
	case
			when "fulfillment status" = 'RTO-Delivered' then '013'
		when trim("order status") = 'Cancelled'
		and "cancellation code" is null then '050'
		when(case
				when trim("order status") = 'Cancelled' then 'Cancelled'
			when trim("order status") = 'Completed' then 'Order-delivered'
			else "fulfillment status"
		end) = 'Cancelled' then "cancellation code"
		else null
	end as "cancellation_code",
	case
		when trim("order status") is null then 'In-progress'
		when trim("order status") = '' then 'In-progress'
		when trim("order status") like '%**%' then 'In-progress'
		else trim("order status")
	end as "Order Status",
		case
		when trim("order status") = 'Completed' then 'Delivered'
		when trim("order status") like '%Return%' then 'Delivered'
		when trim("order status") = 'Cancelled' then 'Cancelled'
		else 'In Process'
	end as "ONDC order_status",
	case
		when upper("Delivery city") like '%XXX%' then 'Undefined'
		when upper("Delivery city") like '' then 'Undefined'
		when upper("Delivery city") like '%*%' then 'Undefined'
		when upper("Delivery city") like 'null' then 'Undefined'
		when upper("Delivery city") is null then 'Undefined'
		else "Delivery city"
	end as "Delivery city",
	case
		when upper("Delivery Pincode") like '%XXX%' then 'Undefined'
		when upper("Delivery Pincode") like '' then 'Undefined'
		when upper("Delivery Pincode") like '%*%' then 'Undefined'
		when upper("Delivery Pincode") like 'null' then 'Undefined'
		when upper("Delivery Pincode") is null then 'Undefined'
		else "Delivery Pincode"
	end as "Delivery Pincode",
	case
		when "Promised time to deliver Date & Time from on_select" is null then 1
		else 0
	end as "TAT null",
	"qty",
	date_parse("f_order-picked-up at date & time",
		'%Y-%m-%dT%H:%i:%s') as "Shipped at",
		date_parse("f_packed at date & time",
		'%Y-%m-%dT%H:%i:%s') as "Ready to Ship",
		date_parse("Promised time to deliver Date & Time from on_select",
		'%Y-%m-%dT%H:%i:%s') as "Promised time"
from
	"default".shared_hudi_b2b_order_fullfillment_view)
select
	"buyer np name",
	"seller np name",
	"network order id",
	"network transaction id",
	"Delivery city",
	"Delivery Pincode",
	"provider_id",
	"TAT null",
	ARRAY_JOIN(ARRAY_AGG(distinct("Domain")),
	',')as "Domain",
	ARRAY_JOIN(ARRAY_AGG(distinct("fulfillment status")),
	',')as "fulfillment status",
	ARRAY_JOIN(ARRAY_AGG(distinct("Order Status")),
	',')as "Order Status",
	ARRAY_JOIN(ARRAY_AGG(distinct("ONDC order_status")),
	',')as "ONDC order_status",
	max("Date") as "Date",
	max("Delivered at") as "Delivered at",
	max("Cancelled at") as "Cancelled at",
	max("Created at") as "Created at",
        case
		when (case
			when ARRAY_JOIN(ARRAY_AGG(distinct("ONDC order_status")),
			',') like '%Process%' then 'In Process'
			else ARRAY_JOIN(ARRAY_AGG(distinct("ONDC order_status")),
			',')
		end
		) = 'Delivered' and max("Delivered at") is null then max("Created at")
		when (case
			when ARRAY_JOIN(ARRAY_AGG(distinct("ONDC order_status")),
			',') like '%Process%' then 'In Process'
			else ARRAY_JOIN(ARRAY_AGG(distinct("ONDC order_status")),
			',')
		end
		) = 'Delivered' then max("Delivered at")
		when (case
			when ARRAY_JOIN(ARRAY_AGG(distinct("ONDC order_status")),
			',') like '%Process%' then 'In Process'
			else ARRAY_JOIN(ARRAY_AGG(distinct("ONDC order_status")),
			',')
		end
		) = 'Cancelled' then max("Cancelled at")
		else max("Created at")
	end as "Updated at",
	max("cancellation_code") as "cancellation_code",
	max("Shipped at") as "Shipped at",
	max("Ready to Ship") as "Ready to Ship",
	coalesce(max("Promised time"),
	max("Created at")) as "Promised time",
        max("provider_name") as "Provider name",
	sum(cast("qty" as bigint)) as "qty"
from
	table1
group by
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8