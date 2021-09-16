package sse.assignment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

class CSVReaderWriter() extends SparkReaderWriter
{
  // Allow the reader to read the CSV files
  def read(filepath:String, schema: StructType, required_column_names:Seq[String]): DataFrame =
  {
    val df = spark
      .read
      .schema(schema)
      .csv(filepath)
    validatePresenceOfColumns(df, required_column_names )
    df
  }
}