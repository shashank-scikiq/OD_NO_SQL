with snp as (select
"bpp_id" as "Seller NP",
"bap_id" as "Buyer NP",
"provider_id",
"network_order_id",
"network_transaction_id",
"item_id",
case
when "delivery_city" like '%XX%' then null
else "delivery_city"
end as "Delivery city",
"order_status" as "Order Status",
"cancellation_code",
"o_created_at_date" as "Created at",
"o_completed_on_date" as "Completed at",
"o_cancelled_at_date" as "Cancelled at"
from "default".shared_gift_card_order_fulfillment)
select * from snp