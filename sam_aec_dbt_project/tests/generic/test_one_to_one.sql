{% test is_one_to_one(model, column_name, field) %}

with validation as (

    select
        {{ column_name }} as test_field,
        count(distinct {{ field }}) as test_count
    
    from {{ model }}
    group by 1
    order by 2 desc

),

validation_errors as (

    select
        test_field,
        test_count

    from validation
    where test_count > 1

)

select *
from validation_errors

{% endtest %}
