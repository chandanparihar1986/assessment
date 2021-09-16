package sse.assignment
import assignment.{cleanup, load_properties}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

class assignment_test extends AnyFlatSpec {

  "The XML file" should " have 1 record with 4 columns" in {
    val prop = load_properties("config/config.properties")

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
    val df = assignment.process_xml(prop, xml_schema)
    assert(df.count() === 1)
    assert(df.columns.length === 4)

  }


  "The JSON file" should " have 1 record with 4 columns" in {
    val prop = load_properties("config/config.properties")

    cleanup(prop)

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
    val df = assignment.process_json(prop, json_schema)
    assert(df.count() === 1)
    assert(df.columns.length === 4)

  }


}
