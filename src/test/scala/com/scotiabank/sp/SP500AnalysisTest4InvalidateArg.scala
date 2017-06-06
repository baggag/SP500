package com.scotiabank.sp

import java.io.File

import com.scotiabank.sp.SP500Analysis._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

/**
  * Created by Gaurav on 6/5/2017.
  * This tests passing an false to the process function for input validation
  */
class SP500AnalysisTest4InvalidateArg extends FlatSpec{

  it should "throw a Exception" in {

    val conf = new SparkConf(false)
      .setMaster("local[*]")
      .set("spark.default.parallelism","1")
      .set("spark.cores.max","1")
       .set("spark.ui.enabled","false")
     implicit val spark = SparkSession.builder().appName("Spark Test").config(conf).getOrCreate()

    val path = "file://"+new File(".").getCanonicalPath+"/testdata/test1.csv"
    assertThrows[Exception] {

      val mainFunc = process(false)(path,90)
    }

  }

}
