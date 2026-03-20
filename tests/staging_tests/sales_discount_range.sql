select *
from {{ ref('stg_sales') }}
where discount<0 or discount>100