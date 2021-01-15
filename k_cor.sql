--k correction----------------------------------------------------------------------------------------------------------

-- таблица с данными из weekly
with weekly as(select weekly.id_product,
                      weekly.rpr,
                      --weekly.total_pv_ab as pv_ab,
                      psb.psb,
                      weekly.pwo_total as pwo
               from data._weekly_dataset(current_date - interval '7 days 3 hours', current_date - interval '3 hours') as weekly
               left join data.da_psb(current_date - interval '7 days 3 hours',current_date - interval '3 hours') psb on weekly.id_product = psb.id_product),
-- таблица с процентом поставок за неделю
slp as(select slp.id_product,
              sum(slp.fact) / sum(case when slp.purchase_nominal is null or slp.package_count is null
                                       then slp.count_original
                                       else slp.purchase_nominal * slp.package_count
                                       end) * 100 as supply
       from data.da_slp(current_date - interval '7 days 3 hours', current_date - interval '3 hours') as slp
       group by slp.id_product),
-- таблица сроков годности
shelflife as(select tproductstorageconditions.id_product,
                    case when tproductstorageconditions.id_shelflifewunit = 8 then 365 * tproductstorageconditions.shelflife
                         when tproductstorageconditions.id_shelflifewunit = 9 then (365::numeric/12::numeric) * tproductstorageconditions.shelflife
                         when tproductstorageconditions.id_shelflifewunit = 10 then 1 * tproductstorageconditions.shelflife
                         when tproductstorageconditions.id_shelflifewunit = 11 then (1::numeric/24::numeric) * tproductstorageconditions.shelflife
                         when tproductstorageconditions.id_shelflifewunit = 12 then 1 * tproductstorageconditions.shelflife
                         else 9999
                         end as shelf_life
             from tproductstorageconditions),
-- вывод таблицы для расчетов
result as(select weekly.id_product,
                 coalesce(slp.supply, 100) as supply,
                 weekly.rpr,
                 weekly.psb,
                 weekly.pwo,
                 shelflife.shelf_life
          from weekly
          left join slp on slp.id_product = weekly.id_product
          left join shelflife on shelflife.id_product = weekly.id_product),
-- настройка расчета коэфициента в зависимости от psb
result_2 as(select result.*,
                   case when result.supply = 0 or result.psb = 0
                        then 100
                        else 100 * result.psb / result.supply
                        end as estimated_pv_ab
            from result),
-- настройка расчета коэфициента в зависимости от pwo
result_3 as(select result_2.*,
                   case when result_2.pwo <= 10 or (result_2.pwo <= 15 and result_2.rpr >= 200)
                        then 100 * 1 / result_2.estimated_pv_ab
                        else 1
                        end as k_element
            from result_2),
-- настройка расчета коэфициента в зависимости от срока годности
result_4 as(select result_3.*,
                   case when result_3.shelf_life * 0.8 < 3
                        then (case when result_3.k_element > 2
                                   then 2
                                   else result_3.k_element
                                   end)
                        when result_3.shelf_life * 0.8 < 15
                        then (case when result_3.k_element > 2.5
                                   then 2.5
                                   else result_3.k_element
                                   end)
                        when result_3.shelf_life * 0.8 < 30
                        then (case when result_3.k_element > 3
                                   then 3
                                   else result_3.k_element
                                   end)
                        else (case when result_3.k_element > 3.5
                                   then 3.5
                                   else result_3.k_element
                                   end)
                        end as k
            from result_3)
select result_4.id_product,
       result_4.k,
       concat('update tdistributionproviderproducts set purchase_correctioncoef = ',result_4.k,' where id_product = ',result_4.id_product,';') as request
from result_4
;