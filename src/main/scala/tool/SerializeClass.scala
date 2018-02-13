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

  //val zipper = udf[Seq[(String, String)], Seq[String], Seq[String]](_.zip(_))
//  val zipper = udf { (code_rome_proche: Seq[String], libelles_type_mobilite: Seq[String]) =>
//    code_rome_proche.zip(libelles_type_mobilite).toArray.map { case (x, y) => CodeRomeProche(x, y) }
//  }

  // val zipperComp = udf{(code_rome: Seq[String], code_rome_libelle: Seq[String]) =>
  //   code_rome.zip(code_rome_libelle).toArray.map{ case (x,y) => CompetencyCodeRome(x,y) }
  // }

  val listFlattenUDF = udf((list: Seq[Seq[String]]) => {
    list.flatten
  })


  // TODO : Refaire au propre pour obtenir un Array et non une string à split
  def listFlatten: (Seq[String] => String) = (list: Seq[String]) => {
    list.mkString(",")
  }

}
