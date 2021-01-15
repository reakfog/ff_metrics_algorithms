create function pv_function(pdatefrom timestamp without time zone DEFAULT NULL::timestamp without time zone, pdateto timestamp without time zone DEFAULT NULL::timestamp without time zone, pid_products integer[] DEFAULT NULL::integer[])
    returns TABLE(id_product bigint, name_product character varying, pv_ab double precision, pv double precision)
    language plpgsql
as
$$
begin
  RETURN query
with all_get_products as (
    select distinct tproducts.id as id_product,
                    tproducts.name as name_product,
                    tproductitems.id_storepoint
                    from tproducts
join tproductitems on tproducts.id = tproductitems.id_product
join tproviderproducts on tproducts.id = tproviderproducts.id_product
join tstorepoints on tproductitems.id_storepoint = tstorepoints.id
where tproducts.deleted is not true and tproducts.id_internal_type is null
and tproductitems.isactive is true and tproductitems.isinsellout is not true and tproviderproducts.isactive is true and
      tstorepoints.isactive is true
    order by id_product
),

all_times as (
select tproducts.id, tproducts.name, tproductitems.id_storepoint, tproductitemsalehistory.date_start, tproductitemsalehistory.date_end,
       case when tproductitemsalehistory.date_start < pdatefrom
           and ((tproductitemsalehistory.date_end > pdatefrom and
                 tproductitemsalehistory.date_end < pdateto)
                 or (tproductitemsalehistory.date_end is null and tproductitemsalehistory.date_start < pdateto) or tproductitemsalehistory.date_end > pdateto)
           then
           make_timestamp(extract(year from pdatefrom :: timestamp) :: int, extract(month from pdatefrom :: timestamp) :: int, extract(day from pdatefrom :: timestamp) :: int, extract(hour from pdatefrom :: timestamp) :: int, 0, 0)
           else tproductitemsalehistory.date_start
           end as new_date_start,

       case when (tproductitemsalehistory.date_end > pdateto
           and tproductitemsalehistory.date_start < pdateto) or (tproductitemsalehistory.date_end is null and tproductitemsalehistory.date_start < pdateto)
           then
        make_timestamp(extract(year from pdateto :: timestamp) :: int, extract(month from pdateto :: timestamp) :: int, extract(day from pdateto :: timestamp) :: int, extract(hour from pdateto :: timestamp) :: int, 0, 0)
           else tproductitemsalehistory.date_end
           end as new_date_end

       from tproductitemsalehistory
left join tproductitems on tproductitems.id = tproductitemsalehistory.id_productitem
left join tproducts on tproducts.id = tproductitems.id_product
join tstorepoints on tstorepoints.id = tproductitems.id_storepoint
join tproviders on tproviders.id = tproductitems.id_provider
where tproducts.id in (select id from tproducts) and tproducts.id_internal_type is null and
      tproducts.deleted is not true and tproductitems.isinsellout is not true and tproductitems.id_storepoint in (select distinct tstorepoints.id from tstorepoints where tstorepoints.isactive is true)
    and tproductitems.isactive is true and tstorepoints.isactive is true and tproviders.isactive is true
    ),

get_value as (
    select all_times.id, all_times.name, all_times.id_storepoint, all_times.date_start, all_times.date_end,
           all_times.new_date_start, all_times.new_date_end,
           extract(epoch from all_times.new_date_end - all_times.new_date_start) as value_diff from all_times
    where (pdatefrom IS NULL or new_date_start >= pdatefrom) and (pdateto IS NULL or new_date_end <= pdateto) and
          (pdateto IS NULL or new_date_end <= pdateto)
),

get_result as (
    select all_get_products.id_product as id, all_get_products.name_product as name, all_get_products.id_storepoint,
           coalesce(sum(get_value.value_diff), 0) as value_diff from all_get_products
    left join get_value on get_value.id = all_get_products.id_product and
                           get_value.id_storepoint = all_get_products.id_storepoint
    group by all_get_products.id_product, all_get_products.name_product, all_get_products.id_storepoint
),

pv as (
    select get_result.id, get_result.name, get_result.id_storepoint, get_result.value_diff / extract(epoch from (pdateto :: timestamp - pdatefrom :: timestamp)) * 100 as pv from get_result
),

table_pv_ab as (
        with table_all_products as (select tproducts.id as id_product, tproducts.name as name_product, tproductitems.id_storepoint, round(count(*) filter ( where ab is true ) * 15.0 * 60.0 :: double precision / extract(epoch from (pdateto :: timestamp - pdatefrom :: timestamp)) :: double precision * 100, 2) as pv_ab,
       round(count(*) filter ( where instant is true ) * 15.0 * 60.0 :: double precision / extract(epoch from (pdateto :: timestamp - pdatefrom :: timestamp)) :: double precision * 100, 2) as pv_storepoint

        from  analytics_product_oos_byproduct
        join tproductitems on analytics_product_oos_byproduct.id_productitem = tproductitems.id
        join tproducts on tproductitems.id_product = tproducts.id
        where
              analytics_product_oos_byproduct.date >= pdatefrom and analytics_product_oos_byproduct.date < pdateto
        group by tproducts.id, tproducts.name, tproductitems.id_storepoint),

        table_all_pv as (
            select table_all_products.id_product,
                   table_all_products.name_product,
                   table_all_products.id_storepoint,
                   table_all_products.pv_ab,
                   table_all_products.pv_storepoint,
                   table_all_products.pv_ab + table_all_products.pv_storepoint as pv from table_all_products
        ),

        table_merge_storepoint as (
            select table_all_pv.id_product, table_all_pv.name_product,
                   table_all_pv.id_storepoint,
                   coalesce(table_all_pv.pv_ab, 0) as pv_ab
            from table_all_pv
        ),

        table_result as (
            select table_merge_storepoint.id_product,
                   table_merge_storepoint.name_product,
                   table_merge_storepoint.id_storepoint,
                   sum(table_merge_storepoint.pv_ab) as pv_ab
                   from table_merge_storepoint
            group by table_merge_storepoint.id_product, table_merge_storepoint.name_product, table_merge_storepoint.id_storepoint
        )

        select table_result.id_product,
               table_result.name_product,
               sum(table_result.pv_ab) / (select count(*) from tstorepoints where isactive is true) :: double precision as pv_ab_total
               from table_result
            group by table_result.id_product, table_result.name_product
    ),

res as (
    select distinct all_times.id as id_product, all_times.name as name_product, all_times.id_storepoint, coalesce(pv.pv, 0) as pv from all_times
    left join pv on pv.id = all_times.id and pv.id_storepoint = all_times.id_storepoint
),

result as (
    select all_get_products.id_product, all_get_products.name_product,
           coalesce(round(coalesce(sum(res.pv) / (select count(*) from tstorepoints where isactive is true), 0), 2), 0) as pv_ab
           from all_get_products
    left join res on res.id_product = all_get_products.id_product and res.id_storepoint = all_get_products.id_storepoint
    group by all_get_products.id_product, all_get_products.name_product
)

select result.id_product :: bigint, result.name_product :: varchar, result.pv_ab :: double precision,
       case when result.pv_ab - table_pv_ab.pv_ab_total < 0 then 0.0 else
        round(result.pv_ab - table_pv_ab.pv_ab_total, 2) :: double precision end as pv
       from result
left join table_pv_ab on table_pv_ab.id_product = result.id_product
;
end
$$;

alter function pv_function(timestamp, timestamp, integer[]) owner to sophiedata;

