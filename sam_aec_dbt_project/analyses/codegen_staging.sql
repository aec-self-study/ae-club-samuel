{{ codegen.generate_base_model(
    source_name='web_tracking',
    table_name='pageviews',
    materialized='view'
) }}
