package sse.assignment

/*
  Provides a common factory object to interact with various file formats.
  Each file format is being handled in a separate class
  ToDo: CSVReader is not fully implemented and hence given here for reference.
  ToDo: Therefore, the case is commented below.
 */
object FileReaderFactory{
  def apply(fileType:String, root_element:String="", xsd_path:String="") = fileType.toUpperCase match {
    case "JSON" => new JsonReaderWriter(root_element)
    //case "CSV" => new CSVReader()
    case "XML" => new XmlReaderWriter(root_element, xsd_path)
  }
}
