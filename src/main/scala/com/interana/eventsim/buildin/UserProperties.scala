package com.interana.eventsim.buildin

import java.time.{LocalDateTime, ZoneOffset}

import com.interana.eventsim.{Constants, TimeUtilities}

object UserProperties {
  // utilities for generating random properties for users

  def randomProps(seed: Long, growthRate: Double, startTime: LocalDateTime) = {
    val secondsSinceRegistration =
      Math.min(new TimeUtilities(seed)
                 .exponentialRandomValue(growthRate * Constants.SECONDS_PER_YEAR)
                 .toInt,
               (Constants.SECONDS_PER_YEAR * 5).toInt)

    val registrationTime   = startTime.minusSeconds(secondsSinceRegistration)
    val firstNameAndGender = RandomFirstNameGenerator.randomThing(seed)
    val location           = RandomLocationGenerator.randomThing(seed)

    Map[String, Any](
      "lastName"     -> RandomLastNameGenerator.randomThing(seed),
      "firstName"    -> firstNameAndGender._1,
      "gender"       -> firstNameAndGender._2,
      "registration" -> registrationTime.toInstant(ZoneOffset.UTC).toEpochMilli,
      "location"     -> location,
      "userAgent"    -> RandomUserAgentGenerator.randomThing(seed)._1
    )
  }

  def randomNewProps(seed: Long, growthRate: Double, startTime: LocalDateTime, dt: LocalDateTime) =
    randomProps(seed, growthRate, startTime) + ("registration" -> dt.toInstant(ZoneOffset.UTC).toEpochMilli)

}
