package org.sparksamples.exploredataset

import breeze.linalg.CSCMatrix
import org.apache.spark.SparkContext
import org.sparksamples.Util
import org.apache.spark.mllib.feature.Word2Vec
import scala.collection.mutable.ListBuffer

/**
  * Created by manpreet.singh on 27/02/16.
  */
object explore_movies {

  def processRegex(input:String):String= {
    val pattern = "^[^(]*".r
    val output = pattern.findFirstIn(input)
    return output.get

  }

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "Explore Users in Movie Dataset")

    val raw_title = org.sparksamples.Util.getMovieDataDF().select("name")
    raw_title.show()

    raw_title.createOrReplaceTempView("titles")
    Util.spark.udf.register("processRegex", processRegex _)
    val processed_titles = Util.spark.sql("select processRegex(name) from titles")
    processed_titles.show()
    val titles_rdd = processed_titles.rdd.map(r => r(0).toString)
    val y = titles_rdd.take(5)
    println(titles_rdd.first())

    //val title_terms = null
    val title_terms = titles_rdd.map(x => x.split(" "))
    title_terms.take(5).foreach(_.foreach(println))
    println(title_terms.count())

    val all_terms_dic = new ListBuffer[String]()
    val all_terms = title_terms.flatMap(title_terms => title_terms).distinct().collect()
    for (term <- all_terms){
      all_terms_dic += term
    }

    println(all_terms_dic.length)
    println(all_terms_dic.indexOf("Dead"))
    println(all_terms_dic.indexOf("Rooms"))

    val all_terms_withZip = title_terms.flatMap(title_terms => title_terms).distinct().zipWithIndex().collectAsMap()
    println(all_terms_withZip.get("Dead"))
    println(all_terms_withZip.get("Rooms"))

    val word2vec = new Word2Vec()
    val rdd_terms = titles_rdd.map(title => title.split(" ").toSeq)
    val model = word2vec.fit(rdd_terms)
    println(model.findSynonyms("Dead", 40))

    val term_vectors = title_terms.map(title_terms => create_vector(title_terms, all_terms_dic))
    term_vectors.take(5).foreach(println)



    //val raw_title = movie_fields.map(movie_fields => movie_fields._2)
    //val pattern = "^[^(]*".r
    //val x = raw_title.filter("\"name\" rlike \"^[^(]*\"")
    //x.show()
    //val proc_title = raw_title.map(raw_title => pattern.findFirstIn(raw_title(0)))
    /*val title = proc_title.map(proc_title => proc_title.get.trim)
    title.take(5).foreach(println)

    val title_terms = title.map(title => title.split(" "))
    title_terms.take(5).foreach(_.foreach(println))
    println(title_terms.count())

    val all_terms_dic = new ListBuffer[String]()
    val all_terms = title_terms.flatMap(title_terms => title_terms).distinct().collect()
    for (term <- all_terms){
      all_terms_dic += term
    }

    println(all_terms_dic.length)
    println(all_terms_dic.indexOf("Dead"))
    println(all_terms_dic.indexOf("Rooms"))

    val all_terms_withZip = title_terms.flatMap(title_terms => title_terms).distinct().zipWithIndex().collectAsMap()
    println(all_terms_withZip.get("Dead"))
    println(all_terms_withZip.get("Rooms"))

    val word2vec = new Word2Vec()
    val rdd_terms = title.map(title => title.split(" ").toSeq)
    val model = word2vec.fit(rdd_terms)
    println(model.findSynonyms("Dead", 40))

    val term_vectors = title_terms.map(title_terms => create_vector(title_terms, all_terms_dic))
    term_vectors.take(5).foreach(println)*/

    sc.stop()
  }

  def create_vector(title_terms:Array[String], all_terms_dic:ListBuffer[String]): CSCMatrix[Int] = {
    var idx = 0
    val x = CSCMatrix.zeros[Int](1, all_terms_dic.length)
    title_terms.foreach(i => {
      if (all_terms_dic.contains(i)) {
        idx = all_terms_dic.indexOf(i)
        x.update(0, idx, 1)
      }
    })
    return x
  }

  def convert(year:String): String = {
    try{
      val mod_year = year.substring(year.length - 4,year.length)
      return mod_year
    }catch {
      case e : Exception => return "1900"
    }
  }

}
