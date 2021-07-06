/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import org.apache.flink.api.common.eventtime.WatermarkStrategy

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.api.common.serialization.SimpleStringSchema
import scala.util.parsing.json.JSONObject
import java.time.Duration


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {

    def extract(x: ObjectNode): Map[String, Any] = {
      val dictionary: Map[String, Any] = Map(
        "eventType" -> x.get("value").get("eventType").asText(),
        "uid" -> x.get("value").get("uid").asText(),
        "timestamp" -> x.get("value").get("timestamp").asInt(),
        "ip" -> x.get("value").get("ip").asText(),
        "impressionId" -> x.get("value").get("impressionId").asText()
      )
      return dictionary
    }

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val schema = new JSONKeyValueDeserializationSchema(false)
    val kafkaSourceDisplay = new FlinkKafkaConsumer[ObjectNode]("displays", schema, properties)
    val kafkaSourceClick = new FlinkKafkaConsumer[ObjectNode]("clicks", schema, properties)

    //  Add watermark strategies for both topics
    kafkaSourceDisplay.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[ObjectNode]
          (Duration.ofSeconds(10)))

    kafkaSourceClick.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[ObjectNode]
          (Duration.ofSeconds(10)))

    // create DataStream
    val display: DataStream[ObjectNode] = env.addSource(kafkaSourceDisplay)
    val click: DataStream[ObjectNode] = env.addSource(kafkaSourceClick)

    // extract info from ObjectNode and convert into Map
    val clean_display: DataStream[Map[String, Any]] = display
      .map(x => extract(x))
    val clean_click: DataStream[Map[String, Any]] = click
      .map(x => extract(x))

    // merge the DataStreams into a single one
    val global_stream: DataStream[Map[String, Any]] = clean_display.union(clean_click)

    // suspectIp is a DataStream of ips that clicked at least 5 times within a minute (~ 1 click/10s)
    val suspectIp: DataStream[Map[String, Any]] = global_stream
      .map(x => (x, 1))
      .keyBy(x => x._1.get("ip"))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .filter(x => x._2 >= 5)
      .map(x => x._1)

    // get the global_stream filtered on ip to avoid suspect duplicates
    val global_stream_filtered_1: DataStream[Map[String, Any]] = global_stream
      .map(x => (x, 1))
      .keyBy(x => x._1.get("ip"))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .filter(x => x._2 < 5)
      .map(x => x._1)


    // suspectUid is a DataStream of uid that were seen at least 2 times within a minute
    val suspectUid: DataStream[Map[String, Any]] = global_stream_filtered_1
      .map(x => (x, 1))
      .keyBy(x => x._1.get("uid"))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .filter(x => x._2 >= 2)
      .map(x => x._1)

    // get the global_stream filtered on ip and uid to avoid suspect duplicates
    val global_stream_filtered_2: DataStream[Map[String, Any]] = global_stream_filtered_1
      .map(x => (x, 1))
      .keyBy(x => x._1.get("uid"))
      .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .filter(x => x._2 < 2)
      .map(x => x._1)

    // suspectImpressionId is a DataStream of ImpressionId that were clicked on 1s or less after their display
    val suspectImpressionId: DataStream[Map[String, Any]] = global_stream_filtered_2
      .map(x => (x, 1))
      .keyBy(x => x._1.get("impressionId"))
      .window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(20)))
      .reduce((x1, x2) => (x1._1, x1._2 + x2._2))
      .filter(x => x._2 >= 2)
      .map(x => x._1)

    val properties2 = new Properties
    properties2.setProperty("bootstrap.servers", "localhost:9092")

    val myProducer = new FlinkKafkaProducer[String](
      "suspects",
      new SimpleStringSchema(),
      properties2
    )

    suspectUid.map(x => JSONObject(x).toString()).addSink(myProducer)
    suspectIp.map(x => JSONObject(x).toString()).addSink(myProducer)
    suspectImpressionId.map(x => JSONObject(x).toString()).addSink(myProducer)


    // print the `suspects` topic
    val kafkaSourceSuspects = new FlinkKafkaConsumer[String]("suspects", new SimpleStringSchema(), properties)
    val suspects: DataStream[String] = env.addSource(kafkaSourceSuspects)
    suspects.print()

    env.execute("Fraudulent clicks and displays detection")
  }
}
