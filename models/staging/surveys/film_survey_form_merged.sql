{% for district_name in district_names() %}

    SELECT
        *,
        '{{ district_name }}' AS district_name
    FROM
        {{ source(
            district_name,
            'film_survey'
        ) }}

    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
