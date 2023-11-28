{% macro template_example(args) %}
    
    {% set query %}
        select true as bool
    {% endset %}

    {% if execute %}
        
        {% set results = run_query(query).columns[0].values()[0] %}
        {{ log('SQL results ' ~ results, info=True) }}

        select
            {{ results }} as is_real
        from a_real_table

    {% endif %}

{% endmacro %}
