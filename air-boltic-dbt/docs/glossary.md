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

{% docs fact_trips %}
The `fact_trips` model captures enriched, trip-level data that aggregates operational and commercial performance for each flight operated through the Air Boltic platform.

Each row represents a single trip (flight) and includes metrics such as:
- Number of customer orders (bookings)
- Total seats booked
- Revenue generated per trip
- Duration of the flight
- Seat utilization percentage (booked / max seats)

This model is critical for:
- Evaluating aircraft and route performance
- Monitoring load factors and optimizing scheduling
- Comparing revenue vs capacity across different regions or plane types
- Identifying high-demand vs underutilized city pairs
- Supporting product decisions around network expansion and dynamic pricing

It complements `fact_orders`, which provides a customer-level lens,
by offering a **supply-side view** optimized for operational monitoring,
business health metrics, and executive-level KPIs.
{% enddocs %}

{% docs duration_min %}
Duration of the trip in minutes, calculated as the difference between `start_timestamp` and `end_timestamp`.
{% enddocs %}

{% docs total_orders %}
Number of distinct customer orders associated with the trip.
{% enddocs %}

{% docs seats_booked %}
Total number of seats booked on the trip.
{% enddocs %}

{% docs trip_revenue %}
Total monetary value of all customer orders associated with the trip.
{% enddocs %}

{% docs utilization_pct %}
Percentage of seats booked relative to the aircraft's total seating capacity.
{% enddocs %}

{% docs fact_orders %}
The `fact_orders` model contains customer-level transactional data from the Air Boltic platform.
Each row represents a single seat booking (order) by a customer, enriched with related metadata 
from the trip and customer dimensions.

This model is essential for:
- Understanding who books, when, and how much they pay
- Segmenting revenue by customer type, geography, or group
- Analyzing pricing strategy effectiveness
- Building conversion and retention funnels
- Supporting growth analytics with customer behavior insights

It complements `fact_trips` by offering a **demand-side view** focused on users and monetization,
while `fact_trips` represents supply-side operational performance.
{% enddocs %}

{% docs booking_ts %}
Timestamp of the trip associated with this order, used as a proxy for booking time.
{% enddocs %}

{% docs int_market_success %}
    Market-level metrics model with one row per destination_city. Includes trip-level and order-level KPIs across 7d, 15d, and 30d lookback windows.
    Designed for evaluating geographic success patterns, growth trends, and early signals for market activation.
{% enddocs %}

{% docs trip_count_7d %}
    Number of unique trips that arrived in the destination_city during the past 7 days.
{% enddocs %}

{% docs avg_utilization_pct_7d %}
    Average seat utilization (booked seats ÷ max capacity) across trips to this market over 7 days.
{% enddocs %}

{% docs total_orders_7d %}
    Total number of booking orders made for trips to the destination_city over the last 7 days.
{% enddocs %}

{% docs total_revenue_7d %}
    Aggregated booking revenue (EUR) generated from orders to this city over the last 7 days.
{% enddocs %}

{% docs avg_ticket_price_7d %}
    Average price (EUR) per booking over trips to the destination_city in the last 7 days.
{% enddocs %}

{% docs pct_confirmed_orders_7d %}
    Proportion of bookings with status = "confirmed" among all orders for trips to this market over 7 days.
{% enddocs %}

{% docs repeat_customer_pct_7d %}
    Proportion of orders placed by repeat customers (i.e., customers who booked more than once) in the past 7 days.
{% enddocs %}

{% docs int_airplane_market_success %}
      This model evaluates the success of airplane makes across different destination markets by aggregating trip and order metrics over multiple rolling time windows (7, 15, 30 days). It helps analyze which aircraft types perform better in specific regions — valuable for fleet planning and route optimization.
{% enddocs %}

{% docs airplane_make %}
The manufacturing make of an airplane (e.g., Boeing 737, Cessna Citation), used here as a proxy for airline or aircraft type performance analysis.
{% enddocs %}

{% docs int_success_summary %}
      A snapshot-level fact model that tracks Air Boltic’s daily performance metrics using a dynamic date spine. It aggregates revenue, trip volume, customer activity, and booking quality for each date — simulating a snapshot-based metric table for time series and trend analysis.

      Useful for:
      - Revenue and volume trend analysis
      - KPI dashboards
      - Retention and operational health checks
      - Baseline input for deviation alerts
{% enddocs %}

{% docs snapshot_date %}
The date used as the snapshot reference, derived from the date spine. Acts as the grain of the model.
{% enddocs %}

{% docs trip_count %}
The total number of unique trips started on this snapshot date.
{% enddocs %}

{% docs order_count %}
The total number of orders associated with trips on this snapshot date.
{% enddocs %}

{% docs total_revenue %}
Sum of all booking revenues (EUR) for trips active on this date.
{% enddocs %}

{% docs avg_ticket_price %}
Average booking price for orders on this snapshot date.
{% enddocs %}

{% docs avg_utilization_pct %}
Average seat utilization percentage across all trips on this snapshot date.
{% enddocs %}

{% docs pct_confirmed_orders %}
Proportion of orders marked as "finished" out of total orders on this snapshot date.
{% enddocs %}

{% docs unique_customers %}
Count of unique customers who placed an order on this snapshot date.
{% enddocs %}

{% docs repeat_customer_count %}
Proportion of customers who placed more than one order on this snapshot date.
{% enddocs %}

{% docs market_level_success_score %}
      Success scoring model at the destination city level.

      It calculates a composite `success_score` based on normalized KPIs over a 30-day window:
      - Seat utilization
      - Revenue
      - Repeat customer rate
      - Booking confirmation rate

      Used to benchmark market performance and identify high-potential regions.
{% enddocs %}

{% docs normalized_utilization %}
Min-max normalized value of `avg_utilization_pct_30d` from the `int_market_success` model. Scaled between 0 and 1 across all cities.
{% enddocs %}

{% docs normalized_revenue %}
Min-max normalized value of `total_revenue_30d` from the `int_market_success` model. Scaled between 0 and 1.
{% enddocs %}

{% docs normalized_repeat_pct %}
Normalized version of the `repeat_customer_pct_30d` metric. Indicates customer stickiness and loyalty per market.
{% enddocs %}

{% docs normalized_confirmed_pct %}
Normalized version of `pct_confirmed_orders_30d`. Represents operational reliability in each market.
{% enddocs %}

{% docs success_score %}
Weighted composite score used to rank market performance. Helps identify patterns of success across Air Boltic's regions.
{% enddocs %}

{% docs last_updated_at %}
Timestamp for when the success score was calculated. Helps with monitoring freshness of data.
{% enddocs %}

{% docs int_customer_segment_analysis %}
      Aggregated behavioral and revenue metrics for each unique customer segment across 7, 15, and 30-day time windows.
      This model enables cohort-style insights into customer preferences by joining customer, trip, and order data.
{% enddocs %}

{% docs segment_id %}
Unique identifier for the customer segment created using a hashed surrogate key across group, airplane, and segment dimensions.
{% enddocs %}

{% docs plane_size_segment %}
Categorization of the aircraft by capacity (e.g., small, medium, large).
{% enddocs %}

{% docs plane_distance_segment %}
Categorization of the aircraft by route distance (e.g., short-haul, long-haul).
{% enddocs %}

{% docs total_orders_15d %}
Count of orders placed by customers in this segment in the past 15 days.
{% enddocs %}

{% docs total_orders_30d %}
Count of orders placed by customers in this segment in the past 30 days.
{% enddocs %}

{% docs unique_customers_7d %}
Number of distinct customers in the segment who placed orders in the last 7 days.
{% enddocs %}

{% docs unique_customers_15d %}
Number of distinct customers in the segment who placed orders in the last 15 days.
{% enddocs %}

{% docs unique_customers_30d %}
Number of distinct customers in the segment who placed orders in the last 30 days.
{% enddocs %}

{% docs total_revenue_15d %}
Total revenue generated from orders in the segment during the past 15 days.
{% enddocs %}

{% docs total_revenue_30d %}
Total revenue generated from orders in the segment during the past 30 days.
{% enddocs %}

{% docs avg_ticket_price_15d %}
Average price of tickets sold to this segment over the past 15 days.
{% enddocs %}

{% docs avg_ticket_price_30d %}
Average price of tickets sold to this segment over the past 30 days.
{% enddocs %}

