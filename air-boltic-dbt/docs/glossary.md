{% docs stg_boltic_customers %}
      This staging model processes customer data ingested from the Air Boltic source system.

      It ensures type consistency by explicitly casting fields to expected types. The model
      deduplicates customers using the `customer_id` as the unique key and is set up as an
      incremental model to reduce compute costs. New customer records are inserted, and
      existing records are updated if customer attributes (e.g., name, email, phone number)
      change.

      The model supports partitioning by the `creation_date` column to optimize query performance
      and is clustered by `customer_group_id` to enhance read efficiency.
{% enddocs %}