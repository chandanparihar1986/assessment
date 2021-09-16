package sse.assignment

import java.io.FileNotFoundException
import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

import scala.io.Source

object assignment {

  def process_xml(prop:Properties, xml_schema:StructType):DataFrame={
    val required_columns_xml = prop.getProperty("xml_req_flds").split(',').toSeq

    val xml_obj = FileReaderFactory("xml", prop.getProperty("xml_root_element"), prop.getProperty("xml_xsd_path"))
    val df = xml_obj.read(prop.getProperty("xml_file_path"), xml_schema, required_columns_xml)
    df.createTempView("xml_activity_temp")
    val mapping_df = xml_obj.spark.read.option("header",true).csv(prop.getProperty("mapping_file_path"))
    mapping_df.createTempView("mapping_temp")
    val output_df = xml_obj.spark.sql(
      """
                                     |SELECT a.userName AS user,
                                     |       websiteName AS website,
                                     |       b.acitivty_description AS activitytypedescription,
                                     |       a.loggedInTime AS signedintime
                                     |FROM xml_activity_temp a
                                     |LEFT JOIN mapping_temp b ON a.activityTypeCode = b.activity_code

                                    """.stripMargin)

    xml_obj.write(output_df, prop.getProperty("output_file_path"))
    output_df
  }

  def process_json(prop:Properties, json_schema:StructType):DataFrame={
    val required_columns_json = prop.getProperty("json_req_flds").split(",").toSeq
    val json_obj = FileReaderFactory("json",  prop.getProperty("json_root_element"))
    val df1 = json_obj.read(prop.getProperty("json_file_path"), json_schema, required_columns_json)
    val output_df = df1.select(col("activity.username").as("user"),col("activity.websiteName").as("website"),
      col("activity.activityTypeDescription").as("activitytypedescription"), col("activity.signedInTime").as("signedintime"))
    json_obj.write(output_df, prop.getProperty("output_file_path"))
    output_df
  }

  def load_properties(config_path:String): Properties ={

    val properties: Properties = new Properties()
    try
      {
        val source = Source.fromFile(config_path)
        properties.load(source.bufferedReader())
      }
    catch
      {
        case ex:Exception => {
        throw new FileNotFoundException("Properties file cannot be loaded")}
      }

    return properties
  }

  // Remove json file(s) if already exists
  // ToDo: Depending on the sink chosen, this operation is most likely not required.
  def cleanup(prop:Properties): Unit ={

    import java.io.File

    import scala.reflect.io.Directory

    val directory = new Directory(new File(prop.getProperty("output_file_path")))

    if(directory.exists == true)
    {
      directory.deleteRecursively()
    }
  }


  // TODO: the below code can further be refactored by isolating some of the code in a separate class & functions
  // ToDo: due to the time constraints, the exception handling part wasn't implemented properly in this release
  def main(args: Array[String]): Unit = {

    if(args.length==0)
      {
        throw new Exception("Missing: Application property file is expected!")
      }

    val prop = load_properties(args(0))

    cleanup(prop)

    val xml_schema = StructType {
      Array(
        StructField("userName", StringType),
        StructField("websiteName", StringType),
        StructField("activityTypeCode", IntegerType),
        StructField("loggedInTime", DateType),
        StructField("number_of_views", IntegerType),
      )
    }

    process_xml(prop, xml_schema)

    val json_schema = StructType{
      Array(StructField("activity",
        StructType{
          Array(
            StructField("userName", StringType),
            StructField("websiteName", StringType),
            StructField("activityTypeDescription", StringType),
            StructField("signedInTime", StringType)
          )
        }
      ))
    }

    process_json(prop, json_schema)
  }

}
