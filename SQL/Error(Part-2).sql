with nhm_calls as (
select
	created_at as "Created At",
	coalesce(domain,
	'no id in calls data') as "Domain",
	coalesce(bpp_id,
	'no id in calls data') as "Bpp ID",
	coalesce(bap_id,
	'no id in calls data') as "Bap ID",
	case
		when LOWER(action) = 'select' then 'on_select'
		when LOWER(action) = 'init' then 'on_init'
		when LOWER(action) = 'confirm' then 'on_confirm'
		else 'no id in calls data'
	end as "Action",
	cast(created_at as VARCHAR) || '-' ||
     coalesce(domain,
	'no id in calls data') || '-' ||
     coalesce(bap_id,
	'no id in calls data') || '-' ||
     coalesce(bpp_id,
	'no id in calls data') || '-' ||
     case
		when LOWER(action) = 'select' then 'on_select'
		when LOWER(action) = 'init' then 'on_init'
		when LOWER(action) = 'confirm' then 'on_confirm'
		else 'no id in calls data'
	end as "joinn",
	SUM(mcount) as "Mcount"
from
	shared_provider_id_daily_aggregations
where
	domain in ('ONDC:RET*',
'ONDC:RET17',
'ONDC:RET1A',
'ONDC:TRV11', 'ONDC:TRV10', 'ONDC:RET1B', 'ONDC:RET15', 'ONDC:AGR10', 'ONDC:RET16', 'nic2004:52110', 'ONDC:RET19', 'ONDC:RET18', 'ONDC:RET10', 'ONDC:RET13', 'ONDC:RET11', 'ONDC:RET14', 'ONDC:RET12', 'ONDC:FIS10', 'ONDC:FIS12')
	and LOWER(action) in ('select', 'init', 'confirm')
	and created_at >= DATE(NOW()) - interval '45' day
group by
	1,
	2,
	3,
	4,
	5
),
Nhm_errors as (
select
	created_At as "Created At",
	case
		when error_code = '' then 'Right code not found'
		when error_code is null then 'Right code not found'
		else error_code
	end as "Error Code",
	action as "Action",
	domain as "Domain",
	bap_id as "Bap ID",
	bpp_id as "Bpp ID",
	coalesce(provider_id,
	'no provider id') as "Provider ID",
	case
		when LOWER(action) like '%select%'
			or LOWER(action) like '%init%'
				or LOWER(action) like '%confirm%' then 'pre-order'
				when LOWER(action) like '%status%'
					or LOWER(action) like '%update%'
						or LOWER(action) like '%cancel%'
							or LOWER(action) like '%issue%' then 'post-order'
							else null
						end as "Order Level",
						bpp_id || '_' || provider_id as "join_column_nhm",
						cast(created_At as VARCHAR) || '-' || domain || '-' || bap_id || '-' || bpp_id || '-' || action as "joinn",
						sum(error_count) as "count"
					from
						shared_transaction_error_codes_aggregations
					where
						domain in ('ONDC:RET*',
'ONDC:RET17',
'ONDC:RET1A',
'ONDC:TRV11', 'ONDC:TRV10', 'ONDC:RET1B', 'ONDC:RET15', 'ONDC:AGR10', 'ONDC:RET16', 'nic2004:52110', 'ONDC:RET19', 'ONDC:RET18', 'ONDC:RET10', 'ONDC:RET13', 'ONDC:RET11', 'ONDC:RET14', 'ONDC:RET12', 'ONDC:FIS10', 'ONDC:FIS12')
							and created_At >= DATE(NOW()) - interval '45' day
						group by
							1,
							2,
							3,
							4,
							5,
							6,
							7,
							8,
							9,
							10
),
Nhm_errors_files as (
select
	ne."Created At",
	ne."Error Code",
	ne."Action",
	ne."Domain",
	ne."Bap ID",
	ne."Bpp ID",
	ne."Provider ID",
	case
		when LOWER(ne."Action") like '%select%'
			or LOWER(ne."Action") like '%init%'
				or LOWER(ne."Action") like '%confirm%' then 'pre-order'
				when LOWER(ne."Action") like '%status%'
					or LOWER(ne."Action") like '%update%'
						or LOWER(ne."Action") like '%cancel%'
							or LOWER(ne."Action") like '%issue%' then 'post-order'
							else null
						end as "Order Level",
						"join_column_nhm" ,
						ne.joinn,
						count as "Count",
						case
							when LOWER(nc.action) = 'on_select' then 'select'
							when LOWER(nc.action) = 'on_init' then 'init'
							when LOWER(nc.action) = 'on_confirm' then 'confirm'
						end as "Base_action"
,
						nc.Mcount
					from
						Nhm_errors ne
					left join nhm_calls nc on
						(lower(ne."joinn") = lower(nc."joinn"))),
anti_join as(
select
	ncc."Created At",
	null as "Error Code",
	ncc."Action",
	ncc."Domain",
	ncc."Bap ID",
	ncc."Bpp ID",
	null as "Provider ID",
	case
		when LOWER(ncc."Action") like '%select%'
			or LOWER(ncc."Action") like '%init%'
				or LOWER(ncc."Action") like '%confirm%' then 'pre-order'
				when LOWER(ncc."Action") like '%status%'
					or LOWER(ncc."Action") like '%update%'
						or LOWER(ncc."Action") like '%cancel%'
							or LOWER(ncc."Action") like '%issue%' then 'post-order'
							else null
						end as "Order Level",
						null as "join_column_nhm" ,
						ncc.joinn,
						0 as Count,
						case
							when ncc.action = 'on_select' then 'select'
							when ncc.action = 'on_init' then 'init'
							when ncc.action = 'on_confirm' then 'confirm'
						end as Base_action,
						ncc."Mcount"
					from
						nhm_calls ncc
					left join Nhm_errors_files nef on
						(lower(nef."joinn") = lower(ncc."joinn"))
					where
						nef.Base_action is null)
(
select
	*
from
	anti_join)
union (
select
*
from
Nhm_errors_files )