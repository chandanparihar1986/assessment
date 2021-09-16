package sse.assignment

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
   The read method allows the factory object to interact with various file readings depending on the file type
   Spark is a common object between all readers
  */
trait SparkReaderWriter extends App{

  def read(filepath:String, schema: StructType, required_column_names:Seq[String]):DataFrame




  def write(df:DataFrame, filepath: String): Unit = {
    df.write.mode("append").json(filepath)
  }

  val spark = SparkSession.builder()
    .master("local")
    .appName("Assessment 1")
    .getOrCreate();

  def validatePresenceOfColumns(df: DataFrame, requiredColNames: Seq[String]): Unit = {
    val c = new DataFrameColumnsChecker(df, requiredColNames)
    c.validatePresenceOfColumns()
  }

}
