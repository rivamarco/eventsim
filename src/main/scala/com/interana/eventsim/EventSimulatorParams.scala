package com.interana.eventsim

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.interana.eventsim.config.ConfigFromFile

case class  EventSimulatorParams(nUsers: Int = 1,
  growthRate: Double = 0.0,
  attritionRate: Double = 0.0,
                                startTime: LocalDateTime = LocalDateTime.now,
                                 endTime: LocalDateTime = LocalDateTime.MAX,
  firstUserId: Int = 1,
  randomSeed: Option[Long] = None,
  configFile: String,
  tag: String,
  kafkaTopic: Option[String] = None,
  kafkaBrokerList: Option[String] = None,
  nsdbHost: Option[String] = None,
  nsdbPort: Option[Int] = Option(7817),
  nsdbDb: Option[String] = Option("eventsim"),
  nsdbNamespace: Option[String] = Option("eventsim"),
  nsdbMetric: Option[String] = Option("users"),
  generateCounts: Boolean =  false,
  generateSimilarSongs: Boolean = false,
  realTime: Boolean = false)

