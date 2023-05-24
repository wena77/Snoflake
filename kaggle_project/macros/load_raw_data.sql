{% macro load_raw_data(source, file) %}

{{ log("Loading data", True) }}

  {% set table_name = file + '_RAW' %}
  {% set csv_file = file + '.csv' %}
  {% set query %}
     BEGIN;
     COPY INTO {{table_name}} FROM @{{ source }}/{{ csv_file }} FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ','  FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);
     COMMIT;
  {% endset %}

  {% do run_query(query) %}

{{ log("Completed loading data", True) }}

{% endmacro %}
