
select *
from {{ ref('stg_products') }}
where UNIT_COST is null
   or UNIT_COST <= 0
