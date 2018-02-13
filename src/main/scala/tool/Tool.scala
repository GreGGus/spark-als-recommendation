package tool

import java.io.InputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext



object Tool {

  /**
    * Get the content of a file from the resources directory
    * @param path location of the file into the resources directory, begin with "/"
    * @return Array[String] The content of the file
    */
  def readPropertyFile(path:String): Array[String] ={
    val stream : InputStream = getClass.getResourceAsStream(path)
    val lines = scala.io.Source.fromInputStream( stream ).getLines.toArray

    return lines
  }

  /**
    * Get Max date Insert of directory
    * @param sc
    * @param path
    * @return
    */
  def getMaxDtInsert(sc:SparkContext,path:String):String= {

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val status = fs.listStatus(new Path(path))

    val result = status.map{ elt =>
      try{
        new String(elt.getPath.getName.split("=")(1)).toLong
      }catch {
        case _ : Throwable => 0
      }
    }.max


    return result.toString
  }

  /**
    * Get List of value of a directory
    * @param sc
    * @param path
    * @return
    */
  def getListValueOfDir(sc:SparkContext,path:String):Array[String]= {

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val status = fs.listStatus(new Path(path))

    val result = status.map{ elt =>
      try{
        new String(elt.getPath.getName.split("=")(1))
      }catch {
        case _ : Throwable => ""
      }
    }


    return result.filter(x=>x.length>0)
  }
}
