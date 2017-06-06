package com.scotiabank.sp

import java.io.File

import com.scotiabank.sp.SP500Analysis._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

/**
  * Created by Gaurav on 6/5/2017.
  * This tests passing an invalid value for probability
  */
class SP500AnalysisTest3InvalidProbability extends FlatSpec{

  it should "throw a NumberFormatException" in {

    val conf = new SparkConf(false)
      .setMaster("local[*]")
      .set("spark.default.parallelism","1")
      .set("spark.cores.max","1")
       .set("spark.ui.enabled","false")
     implicit val spark = SparkSession.builder().appName("Spark Test").config(conf).getOrCreate()

    val path = "file://"+new File(".").getCanonicalPath+"/testdata/test1.csv"
    assertThrows[NumberFormatException] {
      val prob = "sd".toDouble
      val mainFunc = process(validateInputs(path,prob))(path,prob)
    }

  }

}
