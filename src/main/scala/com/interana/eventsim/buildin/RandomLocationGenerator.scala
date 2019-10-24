package com.interana.eventsim.buildin

import com.interana.eventsim.WeightedRandomThingGenerator

import scala.io.{Codec, Source}

/**
  * Randomly generates locations
  */
object RandomLocationGenerator extends WeightedRandomThingGenerator[String] {

  val s     = Source.fromResource("data/CBSA-EST2013-alldata.csv")(Codec("ISO-8859-1"))
  val lines = s.getLines()
  val cbsaRegex = new scala.util.matching.Regex(
    """\d+\,[^\,]*\,[^\,]*\,\"([^\"]+)\"\,M(?:et|ic)ropolitan\ Statistical\ Area\,(\d+)\,.*""",
    "name",
    "pop")
  val fields = for { l <- lines; m <- cbsaRegex findFirstMatchIn l } yield
    (m.group("name"), m.group("pop").toInt.asInstanceOf[Integer])
  fields.foreach(this.add)
  s.close()

}
