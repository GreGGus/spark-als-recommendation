package tool


import scala.util.Random
import java.util.UUID.randomUUID
import org.apache.spark.sql.functions._;

/**
  * Created by gportier on 23/11/2017.
  */
object SerializeClass extends java.io.Serializable {

  def insertRandomUUIDValueUDF: (String => String) = (String) => {
    randomUUID().toString
  }


  // UDF pour remplir de données randoms
  val r = scala.util.Random

  def insertRandomFloatValueUDF: (String => Float) = (Unit) => {
    r.nextFloat()
  }

  def insertRandomIntvalueUDF: (String => Int) = (Unit) => {
    r.nextInt(4)
  }

  def insertUIDStringUDF: (String => String) = (Unit) => {
    randomUUID().toString
  }


  val listFlattenUDF = udf((list: Seq[Seq[String]]) => {
    list.flatten
  })


  def listFlatten: (Seq[String] => String) = (list: Seq[String]) => {
    list.mkString(",")
  }

}
