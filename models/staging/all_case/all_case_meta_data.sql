-- The zzz_case table has data of all cases. Here we will take out the meta information of each case

{% for district_name in district_names() %}

    SELECT
        data ->> 'case_id' AS case_id,
        data -> 'properties' ->> 'case_name' AS case_name,
        data -> 'properties' ->> 'case_type' AS case_type,
        data -> 'properties' ->> 'owner_id' AS owner_id,
        indexed_on,
        data ->> 'closed' AS closed,
        data ->> 'domain' AS domain,
        data ->> 'user_id' AS user_id,
        data ->> 'closed_by' AS closed_by,
        data ->> 'opened_by' AS opened_by,
        '{{ district_name }}' AS district_name,
        data
    FROM
        {{ source(
            district_name,
            'zzz_case'
        ) }}

    {% if not loop.last -%}
        UNION ALL
    {%- endif %}
{% endfor %}
