package com.scotiabank.sp

/**
  * Created by Gaurav on 6/5/2017.
  */
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import scala.util.Try


object SP500Analysis {

  val usage =
    """Usage:spark-submit --class "com.scotiabank.sp.SP500Analysis" --master local[*] ~/SP500/target/scala-2.11/sp500_2.11-1.0.jar <path_to_log_file> <percentage>
      |e.g. spark-submit --class "com.scotiabank.sp.SP500Analysis" --master local[*] ~/SP500/SP500/target/scala-2.11/sp500_2.11-1.0.jar  /home/centos/SP500.csv 90
    """.stripMargin
  def main(args:Array[String]) {
    if (args.length != 2) {
      println(usage)
      sys.exit(1)
    }

    val doubleOf = (x:String) => { try {Some(x.toDouble) }catch{case e: Exception => { sys.error("Incorrect percentage supplied:"+x)
                                                                    sys.exit(1)} };}

    val percent:Double = doubleOf(args(1)).get

    implicit val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val boolProcess = process(validateInputs(args(0),percent))_

    println(boolProcess(args(0),percent))
  }

  def validateInputs(filepath:String,perc:Double):Boolean =
  {
    if(Try(perc.toDouble).isFailure)
    {
      sys.error("Incorrect value for percentage:"+perc)
      sys.exit(1)
    }
    if(!Files.exists(Paths.get(filepath)))
      {
        sys.error("File Path Incorrect:"+filepath)
        sys.exit(1)
      }

    if(perc==0)
    {
      sys.error("Incorrect percentage supplied:"+perc)
      sys.exit(1)
    }
    true
  }


  def process(validatedInput:Boolean)(filepath:String,perc:Double)(implicit  spark:SparkSession):Double = {

    if(!validatedInput)
    {
      sys.error("Inputs validation returned false")
      sys.exit(1)
    }

    val dfSchema = new StructType(Array(StructField("date",DateType,true),StructField("index",DoubleType,true)));
    val percReduced = 100 - perc

    val df = spark.read.option("header","true")
      .option("mode", "DROPMALFORMED")
      .schema(dfSchema)
      .csv(filepath)

    if(df.count()<2)
    {
        sys.error("CSV file should have atleast 2 records")
        sys.exit(1)
    }
    val win = Window.orderBy("date")
    val dfWithPrevious = df.withColumn("previous",lag("index",1,0).over(win))

    import spark.sqlContext.implicits._
    val dfWithPercentage = dfWithPrevious.withColumn("percentage",abs(bround(($"index" - $"previous")*100/$"previous",2)))

    val dfSorted = dfWithPercentage.select(col("percentage")).sort($"percentage".desc).na.drop()
    val checkelem = math.floor(dfSorted.count * percReduced/100).toInt
    val percentage = dfSorted.rdd.zipWithIndex().filter(_._2==checkelem).map(_._1).first()

    percentage.getAs[Double](0)
  }

}
