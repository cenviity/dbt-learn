{% test unique( model, column_name ) %}

select *

from (

    select {{ column_name }}

    from {{ model }}

    where
        {{ column_name }} is not null
        and {{ column_name }} not in ('00000', '11111')

    group by {{ column_name }}

    having count(*) > 1

) validation_errors

{% endtest %}
