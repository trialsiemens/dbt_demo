
select *
from {{ ref('stg_customers') }}
where state is not null
  and length(trim(state)) <> 2
