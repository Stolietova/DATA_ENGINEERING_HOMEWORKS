with source as (
    select * from {{ source('analytics_data', 'iris_dataset') }}
)

select 
    id,
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    species
from source