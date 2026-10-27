{% macro log_model_audit() %}
    {% if execute %}
        {% for res in results %}
            {% if res.node.resource_type == 'model' %}
                {% set model_name = res.node.name %}
                {% set status = res.status %}
                {% set execution_time = res.execution_time %}
                
                {# Dynamically capture whether it's a VIEW, INCREMENTAL, or TABLE #}
                {% set run_type = res.node.config.get('materialized', 'unknown') | upper %}
                
                {# Automatically extract exact rows processed from dbt execution engine #}
                {% set adapter_resp = res.adapter_response %}
                {% set rows_affected = adapter_resp.get('rows_affected', 0) if adapter_resp else 0 %}
                
                {% set audit_query %}
                    insert into financial_service.dwh_audit_log (
                        model_name,
                        run_type,
                        execution_date,
                        start_time,
                        end_time,
                        rows_processed,
                        status,
                        error_message
                    )
                    values (
                        '{{ model_name }}',
                        '{{ run_type }}',
                        current_date,
                        clock_timestamp() - interval '{{ execution_time }} seconds',
                        clock_timestamp(),
                        {{ rows_affected }},
                        '{{ status }}',
                        {{ "'" ~ res.message | replace("'", "''") ~ "'" if res.message else 'NULL' }}
                    );
                {% endset %}
                
                {% set cleaned_query = audit_query | trim %}
                {% if cleaned_query != '' %}
                    {% do run_query(cleaned_query) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}