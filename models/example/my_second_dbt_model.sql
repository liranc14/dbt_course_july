
-- Use the `ref` function to select from other models

{% set my_var = 3 %}

select *, {{my_var}}, liran, 1/0
from {{ ref('my_first_dbt_model') }}
where id = 1
