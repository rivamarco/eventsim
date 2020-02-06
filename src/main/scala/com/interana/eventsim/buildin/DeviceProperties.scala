package com.interana.eventsim.buildin

object DeviceProperties {

  def randomProps =
    Map[String,Any](
      "location" -> RandomLocationGenerator.randomThing)

}
