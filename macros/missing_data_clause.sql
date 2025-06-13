-- filter_in will only select missing data of those columns
-- filter_out will only select non-missing data of those columns
{% macro missing_data_clause(col_names, filter_type="in") -%}
    (
        {%- for col in col_names %}
            {%- if filter_type == "in" -%}
                -- ({{col}} IS NULL OR {{col}}::text = '')
                 (COALESCE(TRIM({{ col }}::text), '') = '')
            {%- else -%}
                -- ({{col}} IS NOT NULL OR {{col}}::text != '')
                (COALESCE(TRIM({{ col }}::text), '') <> '')
            {%- endif -%}
            {%- if not loop.last %}
                {%- if filter_type == "in" -%} OR {%- else -%} AND {%- endif -%}
            {%- endif %}
        {%- endfor %}
    )
{%- endmacro %}