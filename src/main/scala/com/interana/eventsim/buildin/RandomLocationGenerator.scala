package com.interana.eventsim.buildin

import com.interana.eventsim.WeightedRandomThingGenerator

import scala.io.Source

/**
 * Randomly generates locations
 */

object RandomLocationGenerator extends WeightedRandomThingGenerator[String] {

  val s = Source.fromFile("data/us_states.txt")
  val lines = s.getLines()
  for (l <- lines) {
    val fields = l.split(",")
    this.add(fields(0), fields(1).toInt)
  }
  s.close()

}
