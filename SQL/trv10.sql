with base_table as (
select
	"bap_id",
	"bpp_id",
	"network_order_id",
	transaction_std_code,
	"order_status" as "status",
	"vehicle_category",
	date(substring("o_created_date",1,10)) as "created_at",
	case
		when date(coalesce(substring("o_completed_at",1,10), substring("f_ride_ended_at",1,10))) is not null then 1 
		when lower(order_status) like '%complete%' then 1
		else 0
	end as "completed_at",
	case 
		when date(coalesce(substring("o_cancelled_at",1,10), substring("f_ride_cancelled_at",1,10))) is not null then 1
		when lower(order_status) like '%cancel%' then 1
		else 0
	end as "cancelled_at",
		case
		when "f_ride_assigned_at" is not null then 1
		else 0
	end as "assigned_at",
	case 
		when row_number() over(partition by "network_order_id"
		order by "o_created_date") > 1 then 0 else 1
	end as "ride_count"
from
	"default".shared_ride_hailing_order
where date(date_parse("o_created_date",'%Y-%m-%dT%H:%i:%s')) >= date('2024-08-01'))
select
	bap_id,
	bpp_id,
	transaction_std_code,
	vehicle_category,
	created_at as "Date",
	sum(completed_at) as "completed",
	sum(cancelled_at) as "cancelled",
	sum(assigned_at) as "assigned",
	sum(ride_count) as "confirmed"
from base_table
group by 1,2,3,4,5