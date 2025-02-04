import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import com.scala.commons.DimCommons._ 

object DimPipelineLoader {

  // Function to transform data
  def transformData(dimAddressDF: DataFrame, dimStoreDF: DataFrame, dimCalendarDF: DataFrame): DataFrame = {

    // Apply column renaming from DimCommons
    val dimAddressDFRenamed = dimAddressArrayCol.foldLeft(dimAddressDF) { (df, colPair) =>
      if (df.columns.contains(colPair._1)) df.withColumnRenamed(colPair._1, colPair._2) else df
    }

    val dimStoreDFRenamed = dimStoreArrayCol.foldLeft(dimStoreDF) { (df, colPair) =>
      if (df.columns.contains(colPair._1)) df.withColumnRenamed(colPair._1, colPair._2) else df
    }

    val dimCalendarDFRenamed = dimCalendarArrayCol.foldLeft(dimCalendarDF) { (df, colPair) =>
      if (df.columns.contains(colPair._1)) df.withColumnRenamed(colPair._1, colPair._2) else df
    }

    // Define join conditions after renaming
    val joinConditionAddressStore = dimAddressDFRenamed("store_key") === dimStoreDFRenamed("store_id")
    val joinConditionStoreCalendar = dimStoreDFRenamed("store_id") === dimCalendarDFRenamed("cal_key")

    // Join the DataFrames after renaming
    val finalTable = dimAddressDFRenamed
      .join(dimStoreDFRenamed, joinConditionAddressStore, "inner")
      .join(dimCalendarDFRenamed, joinConditionStoreCalendar, "inner")
      .select(finalSelect: _*) // Select the final columns from DimCommons

    // Return the transformed DataFrame
    finalTable
  }

  // Main function
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Dimensional Pipeline Loader")
      .master("local[*]")
      .getOrCreate()

    // Read CSV files
    var dimAddressDF = spark.read.option("header", "true").csv("C:/path_to_data/dim_address.csv")
    var dimStoreDF = spark.read.option("header", "true").csv("C:/path_to_data/dim_store.csv")
    var dimCalendarDF = spark.read.option("header", "true").csv("C:/path_to_data/dim_calendar.csv")

    // Apply transformation
    val transformedDF = transformData(dimAddressDF, dimStoreDF, dimCalendarDF)

    // Show the result
    transformedDF.show(false)

    // Save the transformed DataFrame as a CSV
    transformedDF.write.mode("overwrite").option("header", "true").csv("C:/path_to_data/joined_data.csv")
  }
}
