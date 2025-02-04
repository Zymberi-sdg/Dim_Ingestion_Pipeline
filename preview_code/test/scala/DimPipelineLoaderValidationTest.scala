import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class DimPipelineLoaderTest extends AnyFunSuite {

  var spark: SparkSession = _
  var dimAddressDF: DataFrame = _
  var dimStoreDF: DataFrame = _
  var dimCalendarDF: DataFrame = _

  // Set up the Spark session and read the CSV files
  def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("DimPipelineLoaderTest")
      .master("local[*]") // You may want to use local[*] for parallelism
      .getOrCreate()

    dimAddressDF = spark.read.option("header", "true").csv("C:/path_to_data/dim_address.csv")
    dimStoreDF = spark.read.option("header", "true").csv("C:/path_to_data/dim_store.csv")
    dimCalendarDF = spark.read.option("header", "true").csv("C:/path_to_data/dim_calendar.csv")
  }

  // Simple test for row count
  test("Row count test after transformation") {
    setup()
    
    // Call the transformData method from DimPipelineLoader
    val transformedDF = DimPipelineLoader.transformData(dimAddressDF, dimStoreDF, dimCalendarDF)

    // Here, assume you expect 10 rows after the transformation
    assert(transformedDF.count() == 10, "Row count should be 10 after transformation")
  }

  // Additional test for column names
  test("Check if transformed DataFrame has expected columns") {
    setup()

    // Call the transformData method
    val transformedDF = DimPipelineLoader.transformData(dimAddressDF, dimStoreDF, dimCalendarDF)
    
    // Expected columns in the transformed DataFrame
    val expectedColumns = Seq("store_key","zipcode", "store_name", "units_available", "store_category", "month_name", "quarter_name", "address", "city_name", "state_abbr", "calendar_year")

    // Check if transformed DataFrame contains the expected columns
    assert(transformedDF.columns.toSet == expectedColumns.toSet, "Transformed DataFrame does not have the expected columns")
  }

  // Additional test for data validation (e.g., check if a column has non-null values)
  test("Check if 'store_key' column has no null values after transformation") {
    setup()
    
    // Call the transformData method
    val transformedDF = DimPipelineLoader.transformData(dimAddressDF, dimStoreDF, dimCalendarDF)
    
    // Check if the 'store_key' column has no null values
    val nullCount = transformedDF.filter("store_key IS NULL").count()
    
    // Ensure there are no null values in the 'store_key' column
    assert(nullCount == 0, "The 'store_key' column should not have any null values")
  }

}
