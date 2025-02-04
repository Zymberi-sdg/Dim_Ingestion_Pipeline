package com.scala.commons
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.Column

object DimCommons {

    // Define renaming mappings for the dim_address table
    val dimAddressArrayCol: Array[(String, String)] = Array(
       ("address_key", "store_key"),
       ("street_address", "address"),
       ("city", "city_name"),
       ("state", "state_abbr"),
       ("zip_code", "zipcode")
    )

    // Define renaming mappings for the dim_calendar table
    val dimCalendarArrayCol: Array[(String, String)] = Array(
       ("calendar_key", "cal_key"),
       ("month", "month_name"),
       ("year", "calendar_year"),
       ("quarter", "quarter_name")
    )

    // Define renaming mappings for the dim_store table, now including store_units
    val dimStoreArrayCol: Array[(String, String)] = Array(
        ("store_key", "store_id"),
        ("store_name", "store_name"),
        ("store_type", "store_category"),
        ("city", "store_city"),
        ("state", "store_state"),
        ("store_units", "units_available")  
    )

    val finalSelect: Seq[Column] = Seq(
        col("store_key"),
        col("address"),
        col("city_name"),
        col("state_abbr"),
        col("zipcode"),
        col("store_name"),
        col("store_category"),
        col("units_available"),
        col("month_name"),
        col("calendar_year"),
        col("quarter_name")
    )

}
