{% set product_categories = [
    'coffee_beans',
    'merch',
    'brewing supplies'
    ]
%}
select
    date_trunc(ordered_at, month) as order_month,
    {% for product_category in product_categories -%}
    sum(case when category = '{{ product_category }}' then amount end) as {{product_category | replace(' ', '_')}}_sum
    {%- if product_category != product_categories[-1] -%},{% else %}{% endif %}
    {% endfor %}
from {{ ref('fct_products_sold') }}
