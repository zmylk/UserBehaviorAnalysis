package com.like.flink.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object MyKafkaUtil {

  val prop = new Properties()

  prop.setProperty("bootstrap.servers","hadoop102:9092")
  prop.setProperty("group.id","gmall")

  def getConsumer(topic : String) : FlinkKafkaConsumer[String]= {
    val myKafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),prop)
    myKafkaConsumer
  }

//  def getProducer(topic : String) : FlinkKafkaProducer011[String] ={
//    val myKafkaProducer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String]("hadoop102:9092",topic,new SimpleStringSchema())
//    myKafkaProducer
//  }

}
