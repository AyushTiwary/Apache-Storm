package com.tiwari.scala.storm

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import scala.util.Random

class RandomSentenceSpout extends BaseRichSpout {

  var spoutOutputCollector: SpoutOutputCollector = _
  var random: Random = _

  override def nextTuple(): Unit = {

    Thread.sleep(100)

    val sentences = Array("Knol means a unit of knowledge and Dus comes from Druksh which in sanskrit means tree","Hence, Knoldus stands for tree of knowledge",
      "It is an inspiration that we live with when we build products which deliver high quality business value and share our knowledge extensively via our blogs, conferences, meetups, books and code",
      "Knol means a unit of knowledge and Dus comes from Druksh which in sanskrit means tree","Hence, Knoldus stands for tree of knowledge",
      "It is an inspiration that we live with when we build products which deliver high quality business value and share our knowledge extensively via our blogs, conferences, meetups, books and code")

    val sentence = sentences(random.nextInt(sentences.length))
    spoutOutputCollector.emit(new Values(sentence))
  }

  override def open(conf: java.util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    spoutOutputCollector = collector
    random = Random
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("word"))
  }

}
