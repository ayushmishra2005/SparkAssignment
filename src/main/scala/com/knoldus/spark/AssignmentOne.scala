package com.knoldus.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
  * Created by ayush on 25/7/16.
  */
object AssignmentOne {

  def sparkFlow = {

    val conf = new SparkConf().setAppName("newApp").setMaster("local")
    val sc = new SparkContext(conf)

    //Q1​. Create an RDD (Resilient Distributed Dataset) named pagecounts​from the input file
    val pageCounts = sc.textFile("/home/ayush/workspace/spark_assignment/pagecounts")

    //Q2​. Get the 10 records from the data and write the data which is getting printed/displayed.
    getTenRecordsAndDisplayResult(pageCounts)

    //Q3​. How many records in total are in the given data set ?
    getTotalRecords(pageCounts)

    //Q4​. The first field is the “project code” and contains information about the language of the
    //pages. For example, the project code “en” indicates an English page. Derive an RDD containing
    // only English pages from pagecounts​
    val englishRdd: RDD[String] = getRDDContainingEnglish(pageCounts)

    //Q5​. How many records are there for English pages?
    val englishRecords = englishRdd.count()
    println("Total number of records for English Pages  ::::" + englishRecords)

    //Q6. ​Find the pages that were requested more than 200,000 times in total.
    findPagesRequested(pageCounts)
  }

  private def findPagesRequested(pageCounts: RDD[String]): Unit = {
    val highRequest = pageCounts.map {
      line => {
        val thirdCol = line.split(" ")
        (thirdCol(1), thirdCol(2).toLong)
      }
    }

    val countAdd = highRequest.reduceByKey((x, y) => x + y)
    val highCount = countAdd.filter(_._2 > 200000L).count()
    println("Pages that were requested more than 200,000 times in total ::::" + highCount)
  }

  private def getRDDContainingEnglish(pageCounts: RDD[String]): RDD[String] = {
    val englishRdd = pageCounts.filter(_.split(" ")(0).equals("en"))
    englishRdd
  }

  private def getTotalRecords(pageCounts: RDD[String]): Unit = {
    val total = pageCounts.count()
    println("Total number of records in the given DataSet ::::" + total)
  }

  private def getTenRecordsAndDisplayResult(pageCounts: RDD[String]): Unit = {
    val tenRecords = pageCounts.take(10).toList

    tenRecords foreach println
  }
}

object SparkAssignmentApp extends App {

  val assignmentOne = AssignmentOne
  assignmentOne.sparkFlow
}