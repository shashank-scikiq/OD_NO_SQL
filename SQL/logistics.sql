with master_log as (
select
	"bap_id",
	"bpp_id",
	"order_id",
	"transaction_id",
	"fulfillment_status",
	case
		when latest_order_status = 'Cancelled' 
    then coalesce(REGEXP_EXTRACT(SUBSTRING("cancellation_code", -3, 3),
		'[0-9]+'),
		'052')
		else null
	end as "cancellation_code",
	"latest_order_status",
	"network_retail_order_id",
	case
		when "pick_up_pincode" like '%XXX%' then 'Undefined'
		when "pick_up_pincode" like '' then 'Undefined'
		when "pick_up_pincode" like '%*%' then 'Undefined'
		when "pick_up_pincode" like 'null' then 'Undefined'
		when "pick_up_pincode" is null then 'Undefined'
		else "pick_up_pincode"
	end as "pick_up_pincode",
	case
		when upper("delivery_pincode") like '%XXX%' then 'Undefined'
		when "delivery_pincode" like '' then 'Undefined'
		when "delivery_pincode" like '%*%' then 'Undefined'
		when "delivery_pincode" like 'null' then 'Undefined'
		when "delivery_pincode" is null then 'Undefined'
		else "delivery_pincode"
	end as "delivery_pincode",
	"network_retail_order_category",
	"shipment_type",
	"motorable_distance",
	cod_order,
	"provider_name",
	pickup_tat_duration,
	rts_tat_duration,
	"awb_number",
	date(date_parse(order_created_at,
	'%Y-%m-%dT%H:%i:%s')) as "date",
	date_parse(order_created_at,
	'%Y-%m-%dT%H:%i:%s') as order_created_at,
	date_parse("o_completed_on_date",
	'%Y-%m-%dT%H:%i:%s') as o_completed_on_date,
	date_parse("o_cancelled_at_date",
	'%Y-%m-%dT%H:%i:%s') as o_cancelled_at_date,
	date_parse("Promised time to deliver",
	'%Y-%m-%dT%H:%i:%s') as promised_time_to_deliver,
	date_parse("f_order_delivered_at_date",
	'%Y-%m-%dT%H:%i:%s') as f_order_delivered_at_date,
	date_parse("f_order_picked_up_date",
	'%Y-%m-%dT%H:%i:%s') as f_order_picked_up_date,
	date_parse("f_out_for_delivery_since_date",
	'%Y-%m-%dT%H:%i:%s') as f_out_for_delivery_since_date,
	date_parse("f_ready_to_ship_at_date",
	'%Y-%m-%dT%H:%i:%s') as f_ready_to_ship_at_date,		
	date_parse("f_cancelled_at_date",
	'%Y-%m-%dT%H:%i:%s') as f_cancelled_at_date,		
	date_parse("f_agent_assigned_at_date",
	'%Y-%m-%dT%H:%i:%s') as f_agent_assigned_at_date,		
	date_parse("pickup_tat",
	'%Y-%m-%dT%H:%i:%s') as pickup_tat,		
	date_parse("rts_tat",
	'%Y-%m-%dT%H:%i:%s') as rts_tat,
	date_parse("f_order_picked_up_date_from_fulfillment",
	'%Y-%m-%dT%H:%i:%s') as f_order_picked_up_date_from_fulfillment,		
	date_parse("f_at_pickup_from_date",
	'%Y-%m-%dT%H:%i:%s') as f_at_pickup_from_date,
	date_parse("f_at_delivery_from_date",
	'%Y-%m-%dT%H:%i:%s') as f_at_delivery_from_date
from
	"default".shared_logistics_item_fulfillment_view_with_date
where
order_created_at is not null
	and 
    (case
		when UPPER(on_confirm_sync_response) = 'NACK' then 1
		when on_confirm_error_code is not null then 1
		else 0
	end) = 0
	and 
    (case
		when 
            date(date_parse(order_created_at,
		'%Y-%m-%dT%H:%i:%s')) >= DATE '2024-05-01'
			and f_agent_assigned_at_date is null
			and UPPER(latest_order_status) = 'CANCELLED'
				and (case
					when bpp_id = 'ondc-lsp.olacabs.com' then 'P2P'
					else shipment_type
				end) = 'P2P'
        then 1
				else 0
			end) = 0),
order_table as (
select
	"network order id",
	"buyer np name" as buyer_np,
	max(date_parse("o_accepted at date & time", '%Y-%m-%dT%H:%i:%s')) as "accepted at"
from
	"default".shared_order_fulfillment_nhm_fields_view_hudi
where
	(case 
				when upper(on_confirm_sync_response) = 'NACK' then 1
		when on_confirm_error_code is not null then 1
		else 0
	end) = 0
group by
		1,
		2),
mini_log as (
select
	m1."bap_id",
	m1."bpp_id",
	o1."buyer_np",
	m1."order_id",
	m1."transaction_id",
	m1."fulfillment_status",
	m1.cod_order,
	m1."date",
	m1."order_created_at",	
	m1."cancellation_code",
	m1."latest_order_status",
	m1."network_retail_order_id" as retail_order_id,
	m1."pick_up_pincode",
	m1."delivery_pincode",
	m1."network_retail_order_category" as retail_category,
	case
		when upper("shipment_type") = 'P2H2P'
			and cast(date_format(m1."promised_time_to_deliver",
			'%H') as INTEGER) >= 11 
	then date_add('day',
			1,
			m1."promised_time_to_deliver")
			else m1."promised_time_to_deliver"
		end as "promised_time_to_deliver",
		m1."shipment_type",
		m1."motorable_distance",
		m1."provider_name",
		m1."awb_number",
		case
			when upper("shipment_type") = 'P2H2P'
				and cast(date_format(coalesce(m1."promised_time_to_deliver",
				m1.pickup_tat),
				'%H') as INTEGER) >= 11 
	then coalesce(m1.pickup_tat_duration,
				0) + 86400
				else m1.pickup_tat_duration
			end as pickup_tat_duration,
			m1.rts_tat_duration,
			case
				when upper("shipment_type") = 'P2H2P'
					and cast(date_format(m1.pickup_tat,
					'%H') as INTEGER) >= 11 
	then date_add('day',
					1,
					m1.pickup_tat)
					else m1.pickup_tat
				end as pickup_tat,
				m1.rts_tat,
				m1.f_ready_to_ship_at_date,
				m1.f_at_pickup_from_date,
				m1.f_agent_assigned_at_date,
				coalesce(m1.f_at_delivery_from_date,
				m1.o_completed_on_date,
				m1.f_order_delivered_at_date) as delivered_date,
				coalesce(m1.f_cancelled_at_date,
				m1.o_cancelled_at_date) as cancelled_date,
				coalesce(m1.f_order_picked_up_date_from_fulfillment,
				m1.f_order_picked_up_date,
				m1.f_out_for_delivery_since_date) as picked_date,
				case
					when o1."network order id" = m1.network_retail_order_id then 1
					else 0
				end as linked,
				o1."accepted at"
			from
				master_log m1
			left join order_table o1 on
				o1."network order id" = m1.network_retail_order_id)
select
	*,
	row_number() over (partition by retail_order_id
order by
	order_created_at desc) as dedup,
		case
		when row_number() over (partition by "bpp_id",
			"transaction_id",
			retail_order_id
	order by
			"order_created_at" desc ) > 1 
            then 0
		else 1
	end as log_retail_count,
	case
		when row_number() over (partition by "order_id",
			"transaction_id",
			"bap_id",
			"bpp_id",
			"order_created_at"
	order by
			"order_created_at" desc ) > 1 
            then 0
		else 1
	end as order_count,
	case
		when "latest_order_status" = 'Completed'
		and delivered_date is not null
		and f_ready_to_ship_at_date is not null 
    then date_diff('minute',
		f_ready_to_ship_at_date,
		delivered_date)
		else null
	end as avg_del,
	date_diff('minute',
	"order_created_at",
	"promised_time_to_deliver") as del_tat_duration,
	case
		when "latest_order_status" = 'Completed'
		and delivered_date is not null
		and f_ready_to_ship_at_date is not null 
		then 
		case
			when delivered_date <= "promised_time_to_deliver" then 1
			else 0
		end
		else null
	end as del_adherence,
	case
		when "latest_order_status" = 'Completed'
		and delivered_date is not null
		and f_ready_to_ship_at_date is not null
		and "promised_time_to_deliver" is not null
		and delivered_date > "promised_time_to_deliver"
	then date_diff('second',
		"promised_time_to_deliver",
		"delivered_date")
		else null
	end as del_breach_diff,
	case
		when f_ready_to_ship_at_date is not null 
	then date_diff('second',
		"order_created_at",
		"f_ready_to_ship_at_date")
		else null
	end as rtsvscrt,
	case
		when picked_date is not null 
	then date_diff('second',
		"f_ready_to_ship_at_date",
		coalesce(f_at_pickup_from_date,
		"picked_date"))
		else null
	end as rtsvspck,
	case
		when "latest_order_status" = 'Completed'
		and delivered_date is not null
		and f_ready_to_ship_at_date is not null 
	then date_diff('minute',
		"f_ready_to_ship_at_date",
		"delivered_date")
		else null
	end as delage,
	case
		when "latest_order_status" = 'Cancelled'
	then date_diff('second',
		"order_created_at",
		cancelled_date)
		else null
	end as cancel_secs,
	case
		when not("latest_order_status" in ('Completed', 'Cancelled'))
	then date_diff('minute',
		"order_created_at",
		now())
		else null
	end as in_process_ageing,
	case
		when f_at_pickup_from_date is not null
		and "picked_date" is not null
	then date_diff('minute',
		f_at_pickup_from_date,
		"picked_date")
		else null
	end as rider_waiting,
	case
		when  (f_agent_assigned_at_date is not null and f_ready_to_ship_at_date is not null) then date_diff('minute',f_ready_to_ship_at_date,f_agent_assigned_at_date)
		else null
	end as rider_assigning,
	case
		when pickup_tat is not null
		and (f_at_pickup_from_date is not null
			or picked_date is not null)
	then 
	case
			when coalesce(f_at_pickup_from_date,
			"picked_date")<= pickup_tat then 0
			else 1
		end
		else null
	end as pick_up_breached,
	case
		when pickup_tat is not null
		and (f_at_pickup_from_date is not null
			or picked_date is not null)
	then date_diff('second',
		pickup_tat,
		coalesce(f_at_pickup_from_date,
		"picked_date"))
		else null
	end as pick_up_tat_diff,
		case
			when rts_tat is not null
		and f_ready_to_ship_at_date is not null
		then 
		case
				when f_ready_to_ship_at_date <= rts_tat then 0
			else 1
		end
		else null
	end as rts_breached,
	case
		when rts_tat is not null
		and f_ready_to_ship_at_date is not null
	then date_diff('second',
		rts_tat,
		f_ready_to_ship_at_date)
		else null
	end as rts_tat_diff
from
	mini_log