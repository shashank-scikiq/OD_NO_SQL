with table1 as (
select
	bap_id,
	bpp_id,
	date(searched_date) as "date",
	network_transaction_id,
	s_category_id,
	pick_up_pincode,
	delivery_pincode,
	s_sync_response,
	on_search_received ,on_search_error
from
	"default".shared_logistics_search_on_search
where  date(searched_date) >= date(now()) - interval '10' day
and bpp_id is not null
and not(regexp_like(bpp_id,
	'preprod|preprd|stage|staging'))
	and not(regexp_like(bpp_id,
	'^(init|search|on_search|ondc-prod.uengage.in|ondcseller-prod.costbo.com|sellerconnect.vikrra.in|api-ondc.dlyb.in|seller.udyamwell.in)$'))
	and not(regexp_like(bap_id,
	'preprod|preprd|stage|pramaan|pre.prod|testSeller|staging|testPlan|testtoprod|logistics_buyer|babaElaichiTest'))
	and not(regexp_like(bap_id,
	'^(biz.test.bitsila.com|ondc-connect-test.localzoho.com)$'))
group by 
	bap_id,
	bpp_id,
	date(searched_date),
	network_transaction_id,
	s_category_id,
	pick_up_pincode,
	delivery_pincode,
	s_sync_response,
	on_search_received,
	on_search_error),
table2 as (
select 
bap_id,network_transaction_id, ARRAY_JOIN(ARRAY_AGG(distinct "on_search_received"
order by
	"on_search_received"),
	',') as review
from
	"default".logistics_search_on_search_data_fields
where  date(searched_date) >= date(now()) - interval '10' day
and bpp_id is not null
and not(regexp_like(bpp_id,
	'preprod|preprd|stage|staging'))
	and not(regexp_like(bpp_id,
	'^(init|search|on_search|ondc-prod.uengage.in|ondcseller-prod.costbo.com|sellerconnect.vikrra.in|api-ondc.dlyb.in|seller.udyamwell.in)$'))
	and not(regexp_like(bap_id,
	'preprod|preprd|stage|pramaan|pre.prod|testSeller|staging|testPlan|testtoprod|logistics_buyer|babaElaichiTest'))
	and not(regexp_like(bap_id,
	'^(biz.test.bitsila.com|ondc-connect-test.localzoho.com)$'))
group by 1,2),
main_table as (
select t1.* , t2.review,
	case
		when row_number() over (partition by t1.bap_id,t1.network_transaction_id order by	t1.date) > 1 then 0
		else 1
	end as snp_key,
	case
		when row_number() over (partition by t1.bpp_id,t1.network_transaction_id order by	t1.date) > 1 then 0
		else 1
	end as lnp_key
from table2 t2 left join table1 t1 on t1.bap_id = t2.bap_id and t1.network_transaction_id = t2.network_transaction_id)
select
	bap_id ,
	bpp_id ,
	"date" ,
	s_category_id ,
	pick_up_pincode ,
	delivery_pincode ,
	s_sync_response ,
	on_search_received ,
	on_search_error ,
	review ,
	sum(snp_key) as snp_key ,
	sum(lnp_key) as lnp_key
from
	main_table
group by bap_id ,
	bpp_id ,
	"date" ,
	s_category_id ,
	pick_up_pincode ,
	delivery_pincode ,
	s_sync_response ,
	on_search_received ,
	on_search_error ,
	review