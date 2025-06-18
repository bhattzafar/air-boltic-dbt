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

{% docs customer_id %}
Unique identifier for the customer. Used as the primary key for deduplication in the model.
{% enddocs %}

{% docs customer_name %}
The full name of the customer as provided in the source system.
{% enddocs %}

{% docs customer_group_id %}
Represents the customer’s associated group or segment for business logic or segmentation.
{% enddocs %}

{% docs customer_email %}
Customer’s email address for communication and identification.
{% enddocs %}

{% docs customer_phone_number %}
Most recent phone number on file for the customer, used for contact and verification.
{% enddocs %}

{% docs stg_boltic_customer_groups %}
      This staging model contains distinct customer groups for Air Boltic customers.

      It standardizes group attributes such as type, name, and registry number. The data
      includes companies, private groups, and organizations, and is used to support
      downstream customer segmentation and reporting.
{% enddocs %}

{% docs group_type %}
The classification of the group. Expected values include: company, private group, or organisation.
{% enddocs %}

{% docs customer_group_name %}
The full name of the customer group or organization. Used for display and reporting purposes.
{% enddocs %}

{% docs group_registry_number %}
An optional registry or official identification number for the group. May be blank for informal groups.
{% enddocs %}