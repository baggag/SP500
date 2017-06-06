package com.scotiabank.sp

import java.io.File

import com.scotiabank.sp.SP500Analysis._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FlatSpec

/**
  * Created by Gaurav on 6/5/2017.
  */
class SP500AnalysisTest2ExpectedOutput extends FlatSpec{

  it should "return 2.3 as value" in {

    val conf = new SparkConf(false)
      .setMaster("local[*]")
      .set("spark.default.parallelism","1")
      .set("spark.cores.max","1")
       .set("spark.ui.enabled","false")
     implicit val spark = SparkSession.builder().appName("Spark Test").config(conf).getOrCreate()

    val path = new File(".").getCanonicalPath+"/testdata/test2.csv"
    val mainFunc = process(validateInputs(path,90))(path,90)
    assert(mainFunc == 2.3)

  }

}
