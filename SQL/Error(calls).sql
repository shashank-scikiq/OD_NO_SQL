select created_at,bpp_id,bap_id,"action",sum(mcount) as "mcount" from "default".shared_provider_id_daily_aggregations
where action in ('on_select','select')
group by 1,2,3,4