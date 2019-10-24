package com.interana.eventsim.buildin

import com.interana.eventsim.WeightedRandomThingGenerator

import scala.io.{Codec, Source}

/**
  * Data originally from http://www.census.gov/genealogy/www/data/2000surnames/index.html
  */
object RandomLastNameGenerator extends WeightedRandomThingGenerator[String] {

  val s     = Source.fromResource("data/Top1000Surnames.csv")(Codec("ISO-8859-1"))
  val lines = s.getLines().drop(1)
  for (l <- lines) {
    val fields = l.split(",")
    this.add(fields(0).toLowerCase.capitalize, fields(2).toInt)
  }
  s.close()

}
