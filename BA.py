# Databricks notebook source
# DBTITLE 1,Computation of total revenue per hotel
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     hotel_id,
# MAGIC     hotel_name,
# MAGIC     SUM(amount_spent) AS total_revenue
# MAGIC FROM workspace.default.travel_bookings
# MAGIC WHERE status = 'Booked'
# MAGIC GROUP BY hotel_id, hotel_name
# MAGIC ORDER BY total_revenue DESC;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Average Stay Duration by Location
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     location,
# MAGIC     AVG(stay_duration) AS avg_stay_duration
# MAGIC FROM workspace.default.travel_bookings
# MAGIC WHERE status = 'Booked'
# MAGIC GROUP BY location
# MAGIC ORDER BY avg_stay_duration DESC;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Hotels with the Highest Cancellation Rates
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     hotel_id,
# MAGIC     hotel_name,
# MAGIC     COUNT(CASE WHEN status = 'Cancelled' THEN 1 END) * 100.0 / COUNT(*) AS cancellation_rate_percent
# MAGIC FROM workspace.default.travel_bookings
# MAGIC GROUP BY hotel_id, hotel_name
# MAGIC ORDER BY cancellation_rate_percent DESC;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Seasonal Trends (Group by Month & Location)
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     DATE_FORMAT(booking_date, 'yyyy-MM') AS booking_month,
# MAGIC     location,
# MAGIC     SUM(amount_spent) AS total_revenue
# MAGIC FROM workspace.default.travel_bookings
# MAGIC WHERE status = 'Booked'
# MAGIC GROUP BY booking_month, location
# MAGIC ORDER BY booking_month, total_revenue DESC;
# MAGIC

# COMMAND ----------

# Read data from your table
bookings_df = spark.table("workspace.default.travel_bookings")

# Convert to Pandas for easy plotting
pdf = bookings_df.toPandas()
