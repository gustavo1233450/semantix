package com.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import jdk.nashorn.internal.codegen.types.LongType


object Teste {
    
  
  case class Err404(day:String, request:String,errCod:String)
  case class AllLines(host:String,returnBytes:String)
  
  def er404(line: String): Err404={
    //Retorna a Requisição entre aspas
    val request = line.split("] ")(1).split(" 404")(0)
    
    //Retorna o código do erro 404
    val errCod = line.split(" -").last.split(" ").last
    
    //retorna o dia da requisição
    val timezone = line.split(" ")(3).slice(1,line.split(" ")(3).length())
    val day = timezone.split(":")(0)
    
    //retorna dataframe
    val errDF:Err404 = Err404(day,request,errCod)
    return errDF
  }
  
  def parseLines(line: String): AllLines={
    val fields = line.split(" ")
    
    //retorna o host
    val host = fields(0)
    
    //retorna os bytes
    val returnBytes = fields.last
    
    //retorna Dataframe
    val dfOK:AllLines = AllLines(host,returnBytes)
    return dfOK
  }
  
  
  
  def main(args: Array[String]){
   Logger.getLogger("org").setLevel(Level.ERROR)
   
   val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:///C:/temp")
      .getOrCreate()
    import spark.implicits._ 
      
    
    //Extract
    val aug = spark.sparkContext.textFile("../access_log_Aug95")
    val jul = spark.sparkContext.textFile("../access_log_Jul95")
    val all = aug.union(jul)
    
    val parsedLines = all.map(parseLines)
    val parsedErrLines = all.filter(x => x.contains(" 404 ")).map(er404)
    
    
    //Data
    val AllOKLines = parsedLines.toDS().cache()
    val ErrLines = parsedErrLines.toDS().cache()
    
    //Tables(para fazer de outra forma com spark.sql no exercício 1)
    //AllOKLines.createOrReplaceTempView("parsedLines")
    //ErrLines.createOrReplaceTempView("parsedErrLines")
    
    //Transform
    //exercise 1
    //val countHost = spark.sql("Select count(*) from  (SELECT distinct host from parsedLines)")
    val countHost = AllOKLines.select("host").distinct().count()
    println("Número de Hostes únicos: " + countHost)
    
    //exercise 2
    //val totErrors = parsedErrLines.count()
    val totErrors = ErrLines.select("day").count()
    println("Total de erros 404: " +totErrors)
    
    //exercise 3
    val url = ErrLines.select("request").groupBy("request").count().sort(desc("count")).limit(5)
    println("5 URLS que mais causaram erro 404")
    url.select("request").show()
    
    //exercise 4
    //val codErr = parsedErrLines.map(x => (x._1,x._3)).mapValues(x => (x,1)).reduceByKey((x,y) => (x._1,x._2+y._2))
    val codErrorByDay = ErrLines.select("day").groupBy("day").count().sort(asc("day"))
    println("Quantidade de erros 404 por dia")
    codErrorByDay.show()
    
    //exercise 5
    val totBytes: Long = AllOKLines.filter(AllOKLines("returnBytes") !== "-")
                          .filter(AllOKLines("returnBytes") !== "" )
                          .withColumn("returnBytes",$"returnBytes" cast "Long" as "returnBytes")
                          .select("returnBytes").agg(sum("returnBytes").cast("long")).first().getLong(0)
                          
    println("Total de Bytes retornados:" + totBytes)
   

    
  }
}
