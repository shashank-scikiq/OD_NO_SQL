with table1 as (
select
	"network order id" as "Network order id",
	"buyer np name" as buyer_np,
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
	date_parse("o_accepted at date & time",	'%Y-%m-%dT%H:%i:%s') as "Accepted at"
from "default".shared_order_fulfillment_nhm_fields_view_hudi
where
(case 
				when upper(on_confirm_sync_response) = 'NACK' then 1
				when on_confirm_error_code is not null then 1
			else 0
			end) = 0),
table2 as (
select
	"Network order id",
	buyer_np,
	max("Accepted at") as "Accepted at"
from
	table1
where
	max_record_key = 1
group by
	1,2),
first_table as (
select * from "default".shared_logistics_item_fulfillment_view_with_date
where
	date(date_parse(order_created_at,'%Y-%m-%dT%H:%i:%s')) >= DATE('2024-05-01')
		and date_parse(f_agent_assigned_at_date,'%Y-%m-%dT%H:%i:%s') is null
		and UPPER(latest_order_status) = 'CANCELLED'
		and (case
				when bpp_id = 'ondc-lsp.olacabs.com' then 'P2P'
				else shipment_type
			end) = 'P2P'),
second_table as (
			select * from "default".shared_logistics_item_fulfillment_view_with_date
			except
			select * from first_table),
third_table as (
			select * from second_table
			where not (lower(bpp_id) like '%test%')
			and not(lower(bap_id) like '%test%')
			and not(lower(bpp_id) like '%preprod%')
			and not(lower(bap_id) like '%demoproject%')
			and not(lower(bpp_id) like '%preprod')
			and DATE(SUBSTRING(order_created_at,1,10)) is not null
			and (case 
				when upper(on_confirm_sync_response) = 'NACK' then 1
				when on_confirm_error_code is not null then 1
			else 0
			end) = 0
			and (case 
				when on_confirm_error_code is null then 0
				else 1
			end) = 0
)
select
a.bap_id,
a.bpp_id,
a.provider_id,
a.order_id,
a.fulfillment_id,
a.transaction_id,
a.item_id,
a.fulfillment_status,
a.fulfillment_type,
date_parse(a.order_created_at,'%Y-%m-%dT%H:%i:%s') as order_created_at,
date(date_parse(a.order_created_at,'%Y-%m-%dT%H:%i:%s')) as "Date",
CASE
	WHEN a."domain" = 'nic2004:60232' THEN 1
	ELSE -1
END AS "domain",
date_parse(a.o_accepted_at_date,'%Y-%m-%dT%H:%i:%s') as o_accepted_at_date,
date_parse(a.o_in_progress_from_date,'%Y-%m-%dT%H:%i:%s') as o_in_progress_from_date,
date_parse(a.o_completed_on_date,'%Y-%m-%dT%H:%i:%s') as o_completed_on_date,
date_parse(a.o_cancelled_at_date,'%Y-%m-%dT%H:%i:%s') as o_cancelled_at_date,
date_parse(a.f_pending_from_date,'%Y-%m-%dT%H:%i:%s') as f_pending_from_date,
date_parse(a.f_order_delivered_at_date,'%Y-%m-%dT%H:%i:%s') as f_order_delivered_at_date,
date_parse(a.f_order_picked_up_date,'%Y-%m-%dT%H:%i:%s') as f_order_picked_up_date,
date_parse(a.f_out_for_delivery_since_date,'%Y-%m-%dT%H:%i:%s') as f_out_for_delivery_since_date,
date_parse(a.f_rto_initiated_at_date,'%Y-%m-%dT%H:%i:%s') as f_rto_initiated_at_date,
date_parse(a.f_ready_to_ship_at_date,'%Y-%m-%dT%H:%i:%s') as f_ready_to_ship_at_date,
CAST(NULLIF(REGEXP_REPLACE(a.cancellation_code, '[^0-9]+', ''), '')AS INTEGER) as cancellation_code,
date_parse(a.f_cancelled_at_date,'%Y-%m-%dT%H:%i:%s') as f_cancelled_at_date,
date_parse(a.f_agent_assigned_at_date,'%Y-%m-%dT%H:%i:%s') as f_agent_assigned_at_date,
date_parse(a.f_out_for_pickup_from_date,'%Y-%m-%dT%H:%i:%s') as f_out_for_pickup_from_date,
date_parse(a.f_pickup_failed_at_date,'%Y-%m-%dT%H:%i:%s') as f_pickup_failed_at_date,
date_parse(a.f_at_destination_hub_from_date,'%Y-%m-%dT%H:%i:%s') as f_at_destination_hub_from_date,
date_parse(a.f_delivery_failed_at_date,'%Y-%m-%dT%H:%i:%s') as f_delivery_failed_at_date,
date_parse(a.f_searching_for_agent_from_date,'%Y-%m-%dT%H:%i:%s') as f_searching_for_agent_from_date,
date_parse(a.f_pickup_rescheduled_at_date,'%Y-%m-%dT%H:%i:%s') as f_pickup_rescheduled_at_date,
date_parse(a.f_delivery_rescheduled_at_date,'%Y-%m-%dT%H:%i:%s') as f_delivery_rescheduled_at_date,
date_parse(a.f_in_transit_from_date,'%Y-%m-%dT%H:%i:%s') as f_in_transit_from_date,
date_parse(a.f_rto_delivered_at_date,'%Y-%m-%dT%H:%i:%s') as f_rto_delivered_at_date,
date_parse(a."Promised time to deliver",'%Y-%m-%dT%H:%i:%s') as "Promised time to deliver",
case 
	when UPPER(a.latest_order_status) = 'COMPLETED' then 'Delivered'
	when UPPER(a.latest_order_status) = 'CANCELLED' then 'Cancelled'
	else 'In Process'
end as Log_Ondc_Status,
a.network_retail_order_id,
a.item_category_id,
CASE 
	when a.bpp_id = 'ondc-lsp.olacabs.com' THEN 'P2P'
	ELSE a.shipment_type END as shipment_type,
CASE
        WHEN REGEXP_LIKE(a.pick_up_pincode, '[0-9]+') THEN 
            CASE
                WHEN LENGTH(
                    COALESCE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(a.pick_up_pincode, '[^\d]+', ''),  -- Remove non-numeric characters
                                '\s+', ''  -- Remove spaces
                            ), 
                            '^\s|\s$', ''
                        ), 
                        NULL
                    )
                ) <= 5 THEN -1
                WHEN LENGTH(
                    COALESCE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(a.pick_up_pincode, '[^\d]+', ''),  -- Remove non-numeric characters
                                '\s+', ''  -- Remove spaces
                            ), 
                            '^\s|\s$', ''
                        ), 
                        NULL
                    )
                ) > 6 THEN -1
                ELSE CAST(
                    COALESCE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(a.pick_up_pincode, '[^\d]+', ''),  -- Remove non-numeric characters
                                '\s+', ''  -- Remove spaces
                            ), 
                            '^\s|\s$', ''
                        ), 
                        NULL
                    ) AS INTEGER
                )
            END
        ELSE -1
    END as pick_up_pincode,
CASE
        WHEN REGEXP_LIKE(a.delivery_pincode, '[0-9]+') THEN 
            CASE
                WHEN LENGTH(
                    COALESCE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(a.delivery_pincode, '[^\d]+', ''),  -- Remove non-numeric characters
                                '\s+', ''  -- Remove spaces
                            ), 
                            '^\s|\s$', ''
                        ), 
                        NULL
                    )
                ) <= 5 THEN -1
                WHEN LENGTH(
                    COALESCE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(a.delivery_pincode, '[^\d]+', ''),  -- Remove non-numeric characters
                                '\s+', ''  -- Remove spaces
                            ), 
                            '^\s|\s$', ''
                        ), 
                        NULL
                    )
                ) > 6 THEN -1
                ELSE CAST(
                    COALESCE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(a.delivery_pincode, '[^\d]+', ''),  -- Remove non-numeric characters
                                '\s+', ''  -- Remove spaces
                            ), 
                            '^\s|\s$', ''
                        ), 
                        NULL
                    ) AS INTEGER
                )
            END
        ELSE -1
    END as delivery_pincode,
case
	when a.network_retail_order_category is null then 'Undefined'
	when a.network_retail_order_category = '' then 'Undefined'
	else a.network_retail_order_category
end as network_retail_order_category,
case 
	when a.item_tat is null then a.category_tat
	else a.item_tat
end as tat,
a.on_confirm_sync_response,
a.on_confirm_error_code,
a.on_confirm_error_message,
a.rts_tat_duration,
date_parse(a.rts_tat,'%Y-%m-%dT%H:%i:%s') AS rts_tat,
a.pickup_tat_duration,
date_parse(a.pickup_tat,'%Y-%m-%dT%H:%i:%s') as pickup_tat,
a.motorable_distance ,
a.motorable_distance_type,
CASE WHEN a.f_order_picked_up_date_from_fulfillment LIKE '56%' THEN NULL 
WHEN a.f_order_picked_up_date_from_fulfillment LIKE 'D%' THEN NULL
WHEN a.f_order_picked_up_date_from_fulfillment LIKE '000%' THEN null
ELSE date_parse(a.f_order_picked_up_date_from_fulfillment,'%Y-%m-%dT%H:%i:%s')
END as f_order_picked_up_date_from_fulfillment,
CASE WHEN a.f_order_delivered_at_date_from_fulfillment LIKE '56%' THEN NULL 
WHEN a.f_order_delivered_at_date_from_fulfillment LIKE 'D%' THEN NULL 
WHEN a.f_order_delivered_at_date_from_fulfillment LIKE '000%' THEN NULL 
ELSE date_parse(a.f_order_delivered_at_date_from_fulfillment,'%Y-%m-%dT%H:%i:%s')
END as f_order_delivered_at_date_from_fulfillment,
b."Network order id" as "retail_noi",
b."Accepted at",
b.buyer_np
from
	third_table a
LEFT join table2 b ON upper(a.network_retail_order_id) = upper(b."network order id")