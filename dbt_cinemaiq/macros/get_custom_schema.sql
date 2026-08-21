--
--     get_custom_schema.sql
--     WHY THIS EXISTS:
--     By default, dbt prefixes every custom schema with the target's
--     default schema e.g. target.schema="DEV" + custom schema "marts"
--     becomes "DEV_marts". That's exactly what we want in dev (a safe,
--     clearly-labeled sandbox that can't collide with anything real).

--     But in PROD, we want clean schema names MARTS, STAGING,
--     INTERMEDIATE not "STAGING_marts" (a leftover of profiles.yml's
--     prod target using schema: STAGING as its base). This macro override
--     says: when running against the 'prod' target, use the custom
--     schema name exactly as written, with no prefix. Dev keeps the
--     safer prefixed behavior untouched.


{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}

    {%- elif custom_schema_name is none -%}
        {{ default_schema }}

    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}