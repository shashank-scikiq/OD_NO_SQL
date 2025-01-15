select
	"bap_id",
	"bpp_id",
	searched_date,
	s_fulfillment_type,
	date("searched_date") as "Date",
	"network_transaction_id",
	o_fulfillment_type,
	"s_category_id",
	"pick_up_pincode",
	"delivery_pincode",
	case
		when "s_sync_response" is null
		or "s_sync_response" = '' then 'ACK'
		else "s_sync_response"
	end as "s_sync_response",
	case
		when s_error is null
		or s_error = '' then 'ACK'
		else s_error
	end as s_error,
	"on_search_received",
	"o_category_id",
	case
		when bpp_id = 'bpp.ulip.digiit.ai' then 'P2P'
		when bpp_id  = 'flash-api.shadowfax.in' then 'P2P'
		when bpp_id  = 'ondc.delhivery.com' then 'P2H2P'
		when bpp_id  = 'ondc.dunzo.in' then 'P2P'
		when bpp_id  = 'ondc.ecomexpress.in' then 'P2H2P'
		when bpp_id  = 'ondc.loadshare.net' then 'P2P'
		when bpp_id  = 'ondc.pidge.in' then 'P2P'
		when bpp_id  = 'ondc.qwqer.in' then 'P2P'
		when bpp_id  = 'ondc.shipyaari.com' then 'P2H2P'
		when bpp_id  = 'ondc.theporter.in' then 'P2P'
		when bpp_id  = 'ondc.whizzard.in' then 'P2P'
		when bpp_id  = 'ondc.xbees.in' then 'P2H2P'
		when bpp_id  = 'ondc.zypp.app' then 'P2P'
		when bpp_id  = 'ondc-lsp.olacabs.com' then 'P2P'
		when bpp_id  = 'prod.ondc.adloggs.com' then 'P2P'
		when bpp_id  = 'webapi.magicpin.in/oms_partner/ondc/logistics' then 'P2P'
		when bpp_id  = 'ondc.shiprocket.in' then 'P2H2P'
		else "shipment_type"
	end as "shipment_type",
	"on_search_error"
from
	"default".shared_logistics_search_on_search 
where 
  date("searched_date") >= now() - interval '10' day 
  and not(
    bpp_id like '%preprod%' 
    or bpp_id like '%dunzo%' 
    or bpp_id like '%ithink%' 
    or bpp_id like '%grab%' 
    or bpp_id = 'api-ondc.dlyb.in' 
    or bap_id like '%preprod%' 
    or bap_id like '%test%' 
    or bap_id like '%pramaan%'
  )