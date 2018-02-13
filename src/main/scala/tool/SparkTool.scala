package tool

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by b84164 on 02/08/2017.
  */
object SparkTool {


  /**
    * Delete directory on HDFS
    * @param uri_cluster cluster hostname
    * @param dir_path HDFS path to delete
    */
  def deleteHdfsDirectory(uri_cluster:String, dir_path:String)={
    if(Setup.isWindow()){
      FileUtils.deleteDirectory(new File(dir_path));
    }else{
      val hadoopConf = new org.apache.hadoop.conf.Configuration();
      val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(uri_cluster), hadoopConf)
      try { hdfs.delete(new org.apache.hadoop.fs.Path(dir_path), true) } catch { case _ : Throwable => { } }
    }
  }


  def reloadRDD(sc:SparkContext, dir_path:String, data_rdd:RDD[String]):RDD[String]={

    // Delete old result
    SparkTool.deleteHdfsDirectory(sc.getConf.get("spark.driver.host"), dir_path)

    //Save new
    data_rdd.saveAsTextFile(dir_path)

    //Read File
    sc.textFile(dir_path)
  }


  def reloadDataFrame(sqlContext: SQLContext, dir_path:String, data_df:DataFrame):DataFrame={

    // Delete old result
    SparkTool.deleteHdfsDirectory(sqlContext.sparkContext.getConf.get("spark.driver.host"), dir_path)

    //Save new
    data_df.write.parquet(dir_path)

    //Read File
    sqlContext.read.parquet(dir_path)
  }
}
