package com.interana.eventsim.buildin

object DeviceProperties {

  def randomProps(seed: Long) =
    Map[String, Any](
      "location"  -> RandomLocationGenerator.randomThing(seed),
      "userAgent" -> RandomUserAgentGenerator.randomThing(seed)._1
    )

}
