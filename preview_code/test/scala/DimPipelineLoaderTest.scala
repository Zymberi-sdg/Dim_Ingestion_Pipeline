import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class DimPipelineLoaderTest extends AnyFunSuite {

  var spark: SparkSession = _
  var dimAddressDF: DataFrame = _
  var dimStoreDF: DataFrame = _
  var dimCalendarDF: DataFrame = _

  // Set up the Spark session and read the CSV files
  def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("DimPipelineLoaderTest")
      .master("local[*]")
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
}
