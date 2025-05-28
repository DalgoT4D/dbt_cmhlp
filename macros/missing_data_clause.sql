-- filter_in will only select missing data of those columns
-- filter_out will only select non-missing data of those columns
{% macro missing_data_clause(col_names, filter_type="in") -%}
    (
        {%- for col in col_names %}
            {%- if type == "in" -%}
                (COALESCE(TRIM({{ col }}::text), '') = '')
            {%- else -%}
                (COALESCE(TRIM({{ col }}::text), '') <> '')
            {%- endif -%}
            {%- if not loop.last %}
                {%- if type == "in" -%} OR {%- else -%} AND {%- endif -%}
            {%- endif %}
        {%- endfor %}
    )
{%- endmacro %}