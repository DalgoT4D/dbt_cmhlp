{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {% if 'staging' in node.fqn and node.fqn.index('staging') + 1 < node.fqn | length %}
            {% set prefix = node.fqn[node.fqn.index('staging')] %}
            intermediate_{{ prefix | trim }}

        {# Fallback to default schema if no specific case matches #}
        {% else %}
            {{ default_schema }}
        {% endif %}

    {%- else -%}

        {% if 'elementary' not in custom_schema_name  %}
            {{ default_schema }}_{{ custom_schema_name | trim }}
        {% else %}
            {{ custom_schema_name | trim }}
        {% endif %}

    {%- endif -%}

{%- endmacro %}