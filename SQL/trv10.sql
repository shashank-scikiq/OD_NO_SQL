with base_table as (
select
	"bap_id",
	"bpp_id",
	"network_order_id",
	transaction_std_code,
	ARRAY_JOIN(ARRAY_AGG(distinct("order_status")),
	',') as "status",
	ARRAY_JOIN(ARRAY_AGG(distinct("vehicle_category")),
	',') as "vehicle_category",
	max("o_created_date") as "created_at",
	max("o_completed_at") as "completed_at",
	max("o_cancelled_at") as "cancelled_at",
	max("o_cancellation_code") as "cancellation_code",
	max("f_ride_assigned_at") as "assigned_at",
	max("f_ride_ended_at") as "f_ride_ended_at",
	max("f_ride_cancelled_at") as "f_ride_cancelled_at",
	max("f_ride_enroute_pickup_at") as "f_ride_enroute_pickup_at",
	max("f_ride_arrived_at") as "f_ride_arrived_at"
from
	"default".shared_ride_hailing_order
group by
	1,
	2,
	3,4),
table1 as (select
	bap_id,
	bpp_id,
	"network_order_id",
	transaction_std_code,
	"status",
	case 
		when row_number() over(partition by "network_order_id" order by	"created_at") > 1 then 0 else 1
end as "ride_count",
	lower("vehicle_category") as "vehicle_category",
	date_parse("created_at",'%Y-%m-%dT%H:%i:%s') as "created_at",
	date(date_parse("created_at",'%Y-%m-%dT%H:%i:%s')) as "Date",
	coalesce (date_parse("completed_at",'%Y-%m-%dT%H:%i:%s'),
	date_parse("f_ride_ended_at",'%Y-%m-%dT%H:%i:%s')) as "completed_at",
	coalesce (date_parse("cancelled_at",'%Y-%m-%dT%H:%i:%s'),
	date_parse("f_ride_cancelled_at",'%Y-%m-%dT%H:%i:%s')) as "cancelled_at",
	date_parse("f_ride_enroute_pickup_at",'%Y-%m-%dT%H:%i:%s') as "enroute_to_pickup",
	date_parse("f_ride_arrived_at",'%Y-%m-%dT%H:%i:%s') as "rider_arrived_at",		
	"cancellation_code",
	case
		when "assigned_at" is not null then 1
		else 0
	end as "assigned_at"
from
	base_table)
	select "bap_id",
"bpp_id",
"network_order_id",
"ride_count",
"vehicle_category",
transaction_std_code,
"created_at",
"Date",
case
	when lower("status") in ('complete','completed') then 'COMPLETED'
	when lower("status") = 'cancelled' then 'CANCELLED'
	when "completed_at" is not null then 'COMPLETED'
	when "cancelled_at" is not null then 'CANCELLED'
	else "status"
end as "order_status",
"completed_at",
"cancelled_at",
case
	when
		(case 
			when lower("status") = 'cancelled' then 'CANCELLED'
			when "cancelled_at" is not null then 'CANCELLED'
		end) = 'CANCELLED' 
	then coalesce("cancellation_code",'050') else null
end as "cancellation_code",
"assigned_at",
"enroute_to_pickup",
"rider_arrived_at"
from table1