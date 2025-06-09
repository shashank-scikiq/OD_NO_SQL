select
	bap_id,
	bpp_id,
	provider_name,
	network_order_id,
	date(date_parse("order_created_at",
	'%Y-%m-%dT%H:%i:%sZ')) as "Date",
	date_parse("order_created_at",
	'%Y-%m-%dT%H:%i:%sZ') as order_created_at,
	cast (qty as int) as quant,
	is_on_confirm,
	order_status,
	coalesce (vehicle_category,
	'Undefined') as "Ticket type",
	date_parse("o_complete_on_date",
	'%Y-%m-%dT%H:%i:%sZ') as completed_at,
	date_parse("o_cancelled_at_date",
	'%Y-%m-%dT%H:%i:%sZ') as cancelled_at,
	fulfillment_status,
	cancellation_code,
	transaction_std_code
from
	"default".shared_trv11_order_fulfilment
where
	date(SUBSTRING(order_created_at, 1, 10)) >= date('2025-04-01')
	and is_on_confirm = 'true'