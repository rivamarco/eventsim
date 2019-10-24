package com.interana.eventsim

import java.io.FileOutputStream
import java.time.{Duration, LocalDateTime, ZoneOffset}
import java.util.Properties

import com.interana.eventsim.Main.ConfFromOptions
import kafka.producer.{Producer, ProducerConfig}

import scala.collection.mutable

import com.interana.eventsim.Utilities.{SimilarSongParser, TrackListenCount}
import com.interana.eventsim.buildin.{DeviceProperties, UserProperties}
import com.interana.eventsim.config.ConfigFromFile

object EventSimulator {

  private val sqrtE = Math.exp(0.5)

  private def logNormalRandomValue = Math.exp(TimeUtilities.rng.nextGaussian()) / sqrtE

  var users = new mutable.PriorityQueue[User]()

  private def generateEvents(param: EventSimulatorParams): Unit = {

    ConfigFromFile.configFileLoader(param.configFile)

    lazy val kafkaProducer = if (param.kafkaBrokerList.isDefined) {
      val kafkaProperties = new Properties()
      kafkaProperties.setProperty("metadata.broker.list", param.kafkaBrokerList.get)
      val producerConfig = new ProducerConfig(kafkaProperties)
      Some(new Producer[Array[Byte], Array[Byte]](producerConfig))
    } else None

    var nUsers = param.nUsers

    val out = if (kafkaProducer.nonEmpty) {
      new KafkaOutputStream(kafkaProducer.get, param.kafkaTopic.get)
//    }
//    else if (param.outputFile.isSupplied) {
//      new FileOutputStream(ConfFromOptions.outputFile())
    } else {
      System.out
    }

//      lazy val startTime = if (param.startTimeArg.isDefined) {
//        LocalDateTime.parse(param.startTimeArg.get)
//      } else if (ConfigFromFile.startDate.nonEmpty) {
//        LocalDateTime.parse(ConfigFromFile.startDate.get)
//      } else if (param.from.isDefined) {
//        LocalDateTime.now().minus(param.from.get, ChronoUnit.DAYS)
//      } else {
//        LocalDateTime.now()
//      }
//
//      lazy val endTime = if (param.endTimeArg.isDefined) {
//        LocalDateTime.parse(param.endTimeArg.get)
//      } else if (ConfigFromFile.endDate.nonEmpty) {
//        LocalDateTime.parse(ConfigFromFile.endDate.get)
//      } else if (param.to.isDefined) {
//        LocalDateTime.now().minus(param.to.get, ChronoUnit.DAYS)
//      } else {
//        //a pragmatic definition of infinite
//        LocalDateTime.MAX
//      }

    lazy val nSDbWriter = new NSDbWriter[User](param.nsdbHost.getOrElse("notReacheableHost"),
                                               param.nsdbPort.get,
                                               param.nsdbDb.get,
                                               param.nsdbNamespace.get,
                                               ConfFromOptions.nsdbMetric.toOption.get)(User.nsdbConverter)

    lazy val realTime = ConfFromOptions.realTime.toOption.get || param.endTime == LocalDateTime.MAX

    lazy val sinkToNsdb = ConfFromOptions.nsdbHost.isDefined

    (0 until nUsers).foreach(
      _ =>
        users += new User(
          param.tag,
          ConfigFromFile.alpha * logNormalRandomValue,
          ConfigFromFile.beta * logNormalRandomValue,
          param.startTime,
          ConfigFromFile.initialStates,
          ConfigFromFile.authGenerator.randomThing,
          UserProperties.randomProps,
          DeviceProperties.randomProps,
          ConfigFromFile.levelGenerator.randomThing,
          out
      ))

    val growthRate = ConfigFromFile.growthRate.getOrElse(ConfFromOptions.growthRate.get.get)
    //fixme think about a way to handle a dynamic growth in case of realtime
    if (growthRate > 0 && !realTime) {
      var current = param.startTime
      while (current.isBefore(param.endTime)) {
        val mu = Constants.SECONDS_PER_YEAR / (nUsers * growthRate)
        current = current.plusSeconds(TimeUtilities.exponentialRandomValue(mu).toInt)
        users += new User(
          param.tag,
          ConfigFromFile.alpha * logNormalRandomValue,
          ConfigFromFile.beta * logNormalRandomValue,
          current,
          ConfigFromFile.initialStates,
          ConfigFromFile.newUserAuth,
          UserProperties.randomNewProps(current),
          DeviceProperties.randomProps,
          ConfigFromFile.newUserLevel,
          out
        )
        nUsers += 1
      }
    }
    System.err.println("Initial number of users: " + ConfFromOptions.nUsers() + ", Final number of users: " + nUsers)

    val startTimeString = param.startTime.toString
    val endTimeString   = param.endTime.toString
    System.err.println("Start: " + startTimeString + ", End: " + endTimeString)

    var lastTimeStamp = System.currentTimeMillis()

    def showProgress(n: LocalDateTime, users: Int, e: Int): Unit = {
      if ((e % 10000) == 0) {
        val now  = System.currentTimeMillis()
        val rate = 10000000 / (now - lastTimeStamp)
        lastTimeStamp = now
        val message = // "Start: " + startTimeString + ", End: " + endTimeString + ", " +
          "Now: " + n.toString + ", Events:" + e + ", Rate: " + rate + " eps"
        System.err.write("\r".getBytes)
        System.err.write(message.getBytes)
      }
    }

    System.err.println("Starting to generate events.")
    System.err.println("Damping=" + ConfigFromFile.damping + ", Weekend-Damping=" + ConfigFromFile.weekendDamping)

    var clock  = param.startTime
    var events = 1

    while (clock.isBefore(param.endTime)) {

      if (realTime) {
        val now = LocalDateTime.now()
        val dif = Duration.between(now, clock)
        if (dif.isNegative)
          Thread.sleep(-dif.getSeconds)
      }

      showProgress(clock, users.length, events)
      val u = users.dequeue()
      val prAttrition = nUsers * ConfFromOptions.attritionRate() *
        (param.endTime.toEpochSecond(ZoneOffset.UTC) - param.startTime.toEpochSecond(ZoneOffset.UTC) / Constants.SECONDS_PER_YEAR)

      clock =
        if (realTime) LocalDateTime.now()
        else u.session.nextEventTimeStamp.get

      if (clock.isAfter(param.startTime))
        if (sinkToNsdb) nSDbWriter.write(u)
        else
          u.writeEvent()
      u.nextEvent(prAttrition)
      users += u
      events += 1
      out.flush()
    }

    System.err.println("")
    System.err.println()

    out.close()
  }

  def compute(param: EventSimulatorParams) = {
    if (param.generateCounts)
      TrackListenCount.compute()
    else if (param.generateSimilarSongs)
      SimilarSongParser.compute()
    else
      this.generateEvents(param)
  }
}
