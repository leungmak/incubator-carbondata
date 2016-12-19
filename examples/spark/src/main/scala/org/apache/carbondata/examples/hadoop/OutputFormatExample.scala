package org.apache.carbondata.examples.hadoop

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.carbondata.hadoop.CarbonInputFormat
import org.apache.carbondata.hadoop.api.{CarbonTableOutputFormat, CarbonTableWriteSupport, DictionaryOutputFormat}
import org.apache.carbondata.spark.SparkRowWriteSupport

object OutputFormatExample {

  private val sc = new SparkContext(new SparkConf()
      .setAppName("OutputFormatExample")
      .setMaster("local[*]"))
  private val rdd = sc.parallelize(1 to 100).map(x => (x, x.toString))

  def main(args: Array[String]): Unit = {

    val pwd = new File("").getCanonicalPath

    // write CarbonData files without generating global dictionary
    writeCarbonFileWithoutDictionary(s"$pwd/t1")
    readCarbonFile(s"$pwd/t1")

    // write CarbonData files with global dictionary
    writeCarbonFileWithDictionary(s"$pwd/t2")
    readCarbonFile(s"$pwd/t2")
  }

  private def readCarbonFile(path: String) = {
    val input = sc.newAPIHadoopFile(
      path,
      classOf[CarbonInputFormat[Array[Object]]],
      classOf[Void],
      classOf[Array[Object]]
    )
    val result = input.map(x => x._2.toList).collect
    result.foreach(x => println(x.mkString(", ")))
  }

  private def writeCarbonFileWithoutDictionary(path: String) = {
    val conf = new Configuration()

    // write carbondata file for the input RDD to `path` folder
    CarbonTableOutputFormat.setWriteSupportClass(conf, classOf[SparkRowWriteSupport])
    CarbonTableWriteSupport.setSchema(conf, null)
    rdd.saveAsNewAPIHadoopFile(
      path,
      classOf[Void],
      classOf[Array[Object]],
      classOf[CarbonTableOutputFormat[Array[Object]]],
      conf
    )
  }

  private def writeCarbonFileWithDictionary(path: String) = {
    val conf = new Configuration()

    // write global dictionary for the input RDD to `path` folder
    rdd.saveAsNewAPIHadoopFile(
      path,
      classOf[String],
      classOf[Integer],
      classOf[DictionaryOutputFormat],
      conf
    )

    // write carbondata file, use global dictionary in the specified `path` folder
    CarbonTableOutputFormat.setWriteSupportClass(conf, classOf[SparkRowWriteSupport])
    CarbonTableWriteSupport.setSchema(conf, null)
    rdd.saveAsNewAPIHadoopFile(
      path,
      classOf[Void],
      classOf[Array[Object]],
      classOf[CarbonTableOutputFormat[Array[Object]]],
      conf
    )
  }
}
