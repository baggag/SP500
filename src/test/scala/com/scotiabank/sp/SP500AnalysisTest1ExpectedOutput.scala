package com.scotiabank.sp

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec
import SP500Analysis._

/**
  * Created by Gaurav on 6/5/2017.
  * This tests a given sample input file and checks if it results the expected output
  */
class SP500AnalysisTest1ExpectedOutput extends FlatSpec{

  it should "return 15.09 as value" in {

    val conf = new SparkConf(false)
      .setMaster("local[*]")
      .set("spark.default.parallelism","1")
      .set("spark.cores.max","1")
       .set("spark.ui.enabled","false")
     implicit val spark = SparkSession.builder().appName("Spark Test").config(conf).getOrCreate()

    val path = new File(".").getCanonicalPath+"/testdata/test1.csv"
    val mainFunc = process(validateInputs(path,90))(path,90)
    assert(mainFunc == 15.09)

  }

}
