{% macro load_raw_data(source) %}

{{ log("Loading data", True) }}

  {% set table_name = source + '_RAW' %}
  {% set csv_file = source + '.csv' %}
  {% set query %}
     BEGIN;
     COPY INTO {{table_name}} FROM @ecommerce/{{csv_file}} FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);
     COMMIT;
  {% endset %}

  {% do run_query(query) %}

{{ log("Completed loading data", True) }}

{% endmacro %}