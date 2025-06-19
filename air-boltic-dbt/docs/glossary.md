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

{% docs stg_boltic_planes %}
      This model contains the list of airplanes operated by Air Boltic, along with their assigned IDs, models, and manufacturers.

      Each record represents a unique aircraft in the system and is validated against detailed manufacturer data to ensure consistency.
{% enddocs %}

{% docs airplane_id %}
A unique identifier for each airplane in the Air Boltic fleet. This ID is used to join airplane records with related details.
{% enddocs %}

{% docs airplane_model %}
The commercial model name of the airplane (e.g., A320neo, 787-9). Used to group aircraft by design.
{% enddocs %}

{% docs manufacturer %}
The company that manufactured the airplane. This field links to the stg_plane_details model to retrieve specs like engine type and capacity.
{% enddocs %}


{% docs stg_plane_details %}
      This model stores technical specifications and design information for all known airplane models used by Air Boltic.

      Each entry represents a unique combination of make and model, providing details about performance and capacity.
{% enddocs %}

{% docs airplane_details_id %}
A unique identifier for each airplane detail entry. Used to distinguish distinct combinations of make and model.
{% enddocs %}

{% docs make %}
The name of the airplane manufacturer (e.g., Boeing, Airbus). Combined with model to identify an aircraft type.
{% enddocs %}

{% docs model %}
The specific aircraft model made by the manufacturer (e.g., A350-900, 737-800).
{% enddocs %}

{% docs engine_type %}
The type of engine installed in the airplane (e.g., Rolls-Royce Trent XWB, CFM LEAP-1A).
{% enddocs %}

{% docs max_distance %}
The maximum flight distance the aircraft can travel on a full tank, measured in nautical miles.
{% enddocs %}

{% docs max_seats %}
The maximum number of passenger seats configured for this aircraft model.
{% enddocs %}

{% docs max_weight %}
The maximum takeoff weight of the aircraft in kilograms.
{% enddocs %}

{% docs stg_boltic_trips %}
      This staging model contains flight-level data for all scheduled or completed trips in the Air Boltic system.

      Each record represents a unique trip, including origin and destination, aircraft used, and timing details.
{% enddocs %}

{% docs trip_id %}
A unique identifier for each flight trip. Serves as the primary key of the stg_boltic_trips model.
{% enddocs %}

{% docs origin_city %}
The city from which the flight originated. Typically the city where the departure airport is located.
{% enddocs %}

{% docs destination_city %}
The city where the flight is scheduled to land or has landed. Used for routing and reporting purposes.
{% enddocs %}

{% docs start_timestamp %}
The exact timestamp at which the airplane took off or was scheduled to take off.
{% enddocs %}

{% docs end_timestamp %}
The exact timestamp when the flight landed or was scheduled to land.
{% enddocs %}

{% docs trip_date %}
The calendar date (in YYYY-MM-DD) format associated with the flight. Often derived from `start_timestamp` and used for partitioning.
{% enddocs %}

{% docs stg_boltic_orders %}
      This staging model represents customer orders for flights in the Air Boltic system.

      Each row corresponds to a booked order, capturing pricing, seat assignment, and status information, along with references to the customer and trip details.
{% enddocs %}

{% docs order_id %}
A unique identifier for each order placed by a customer. Serves as the primary key of this model.
{% enddocs %}

{% docs price_eur %}
The price of the order in euros. Represents the cost paid by the customer for the flight.
{% enddocs %}

{% docs seat_number %}
The seat assigned to the customer for the trip. This value is typically alphanumeric (e.g., "12A").
{% enddocs %}

{% docs status %}
The current status of the order. Example statuses may include `booked`, `cancelled`, or `completed`.
{% enddocs %}


{% docs stg_boltic_customers_non_pii_enriched %}
      This model enriches the customer dataset by:
      - Pulling in non-PII attributes from the staging layer
      - Joining with customer group metadata
      - Deriving `country` from `country_code` using a seed mapping
      It ensures personal data like email and phone numbers are excluded, supporting GDPR compliance.
{% enddocs %}

{% docs email_domain %}
The domain portion of the customer’s email address (e.g., gmail.com), extracted for analytical use.
{% enddocs %}

{% docs country_code %}
The dialing prefix (e.g., +49, +1) extracted from the customer's phone number.
{% enddocs %}

{% docs customer_country %}
The derived country name (in lowercase) that corresponds to the customer's phone country code.
{% enddocs %}

{% docs dim_planes %}
      This model enriches airplane metadata by joining basic plane identifiers with detailed specifications like engine type, seating capacity, and range. It merges `stg_boltic_planes` with `stg_plane_details` using cleaned manufacturer and model names. Deduplication is handled using airplane ID and manufacturer.
{% enddocs %}

