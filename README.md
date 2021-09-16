# SSE Assessment #1

## Prepare Environment
- JDK 1.8
- Scala 1.12
- Spark 3.1.1
- Editor - Intellij or Eclipse
- for all reference libraries, please go through the pom.xml file

## Config file
- project configuration file is given under "config/config.properties"

## Test Data
- Test data is given under project's root directory under data sub-directory



## Build & Run Instructions
- One can build the project jar using "mvn install" if required
- To run the application, use the following command for reference
spark-submit --master local --class sse.assignment.assignment <application jar path> <configuration file>
- Application jar file -> Use the assembled jar
- Configuration file -> config/config.properties (please provide the absolute path for each file path given in the config file)

## Unit Test Instructions
- Test cases are written under src/test/scala/assignment_test
