package com.interana.eventsim

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import com.interana.eventsim.config.ConfigFromFile
import org.rogach.scallop.{ScallopConf, ScallopOption}

object Main extends App {

  private[eventsim] object ConfFromOptions extends ScallopConf(args) {
    val nUsers: ScallopOption[Int] =
      opt[Int]("nusers", descr = "initial number of users", required = false, default = Option(1))

    val growthRate: ScallopOption[Double] =
      opt[Double]("growth-rate",
                  descr = "annual user growth rate (as a fraction of current, so 1% => 0.01)",
                  required = false,
                  default = Option(0.0))

    val attritionRate: ScallopOption[Double] =
      opt[Double]("attrition-rate",
                  descr = "annual user attrition rate (as a fraction of current, so 1% => 0.01)",
                  required = false,
                  default = Option(0.0))

    val startTimeArg: ScallopOption[String] =
      opt[String]("start-time",
                  descr = "start time for data",
                  required = false,
                  default = Option(LocalDateTime.now().minus(14, ChronoUnit.DAYS).toString))

    val endTimeArg: ScallopOption[String] =
      opt[String]("end-time",
                  descr = "end time for data",
                  required = false,
                  default = Option(LocalDateTime.now().minus(7, ChronoUnit.DAYS).toString))

    val from: ScallopOption[Int] =
      opt[Int]("from", descr = "from x days ago", required = false, default = Option(15))

    val to: ScallopOption[Int] =
      opt[Int]("to", descr = "to y days ago", required = false, default = Option(1))

    val firstUserId: ScallopOption[Int] =
      opt[Int]("userid", descr = "first user id", required = false, default = Option(1))

    val randomSeed: ScallopOption[Int] =
      opt[Int]("randomseed", descr = "random seed", required = false)

    val configFile: ScallopOption[String] =
      opt[String]("config", descr = "config file", required = true)

    val tag: ScallopOption[String] =
      opt[String]("tag", descr = "tag applied to each line (for example, A/B test group)", required = false)

    val verbose = toggle("verbose",
                         default = Some(false),
                         descrYes = "verbose output (not implemented yet)",
                         descrNo = "silent mode")
    val outputFile: ScallopOption[String] = trailArg[String]("output-file", required = false, descr = "File name")

    val kafkaTopic: ScallopOption[String] =
      opt[String]("kafkaTopic", descr = "kafka topic", required = false)

    val kafkaBrokerList: ScallopOption[String] =
      opt[String]("kafkaBrokerList", descr = "kafka broker list", required = false)

    val nsdbHost: ScallopOption[String] =
      opt[String]("nsdb-host", descr = "Nsdb Host", required = false)

    val nsdbPort: ScallopOption[Int] =
      opt[Int]("nsdb-port", descr = "Nsdb Port", required = false, default = Option(7817))

    val nsdbDb: ScallopOption[String] =
      opt[String]("nsdb-db", descr = "Nsdb Db", required = false, default = Option("eventsim"))

    val nsdbNamespace: ScallopOption[String] =
      opt[String]("nsdb-namespace", descr = "Nsdb Host", required = false, default = Option("eventsim"))

    val nsdbMetric: ScallopOption[String] =
      opt[String]("nsdb-metric", descr = "Nsdb Host", required = false, default = Option("users"))

    val generateCounts = toggle("generate-counts",
                                default = Some(false),
                                descrYes = "generate listen counts file then stop",
                                descrNo = "run normally")

    val generateSimilarSongs = toggle("generate-similars",
                                      default = Some(false),
                                      descrYes = "generate similar song file then stop",
                                      descrNo = "run normally")

    val realTime =
      toggle("continuous", default = Some(false), descrYes = "continuous output", descrNo = "run all at once")

    verify()
  }

  private[eventsim] lazy val startTime = if (ConfFromOptions.startTimeArg.isSupplied) {
    LocalDateTime.parse(ConfFromOptions.startTimeArg())
  } else if (ConfigFromFile.startDate.nonEmpty) {
    LocalDateTime.parse(ConfigFromFile.startDate.get)
  } else if (ConfFromOptions.from.isSupplied) {
    LocalDateTime.now().minus(ConfFromOptions.from(), ChronoUnit.DAYS)
  } else {
    LocalDateTime.now()
  }

  private lazy val endTime = if (ConfFromOptions.endTimeArg.isSupplied) {
    LocalDateTime.parse(ConfFromOptions.endTimeArg())
  } else if (ConfigFromFile.endDate.nonEmpty) {
    LocalDateTime.parse(ConfigFromFile.endDate.get)
  } else if (ConfFromOptions.to.isSupplied) {
    LocalDateTime.now().minus(ConfFromOptions.to(), ChronoUnit.DAYS)
  } else {
    //a pragmatic definition of infinite
    LocalDateTime.MAX
  }

  private var nUsers = ConfigFromFile.nUsers.getOrElse(ConfFromOptions.nUsers())

  private[eventsim] lazy val seed =
    if (ConfFromOptions.randomSeed.isSupplied)
      ConfFromOptions.randomSeed.toOption.get.toLong
    else
      ConfigFromFile.seed

  private lazy val tag: String =
    if (ConfFromOptions.tag.isSupplied)
      ConfFromOptions.tag.toOption.get
    else
      ConfigFromFile.tag.getOrElse("")

  private[eventsim] lazy val growthRate =
    if (ConfFromOptions.growthRate.isSupplied)
      ConfFromOptions.growthRate.get
    else
      ConfigFromFile.growthRate

  private val params = EventSimulatorParams(
    nUsers = nUsers,
    growthRate = growthRate.getOrElse(0.0),
    attritionRate = ConfFromOptions.attritionRate.getOrElse(0.0),
    startTime = startTime,
    endTime = endTime,
    firstUserId = ConfFromOptions.firstUserId.getOrElse(1),
    randomSeed = Some(seed), //ConfigFromFile.seed,
    configFile = ConfFromOptions.configFile.toOption.get,
    tag = tag,
    kafkaTopic = ConfFromOptions.kafkaTopic.toOption,
    kafkaBrokerList = ConfFromOptions.kafkaBrokerList.toOption,
    nsdbHost = ConfFromOptions.nsdbHost.toOption,
    nsdbPort = ConfFromOptions.nsdbPort.toOption,
    nsdbDb = ConfFromOptions.nsdbDb.toOption,
    nsdbNamespace = ConfFromOptions.nsdbNamespace.toOption,
    nsdbMetric = ConfFromOptions.nsdbMetric.toOption,
    generateCounts = ConfFromOptions.generateCounts.getOrElse(false),
    generateSimilarSongs = ConfFromOptions.generateSimilarSongs.getOrElse(false),
    realTime = ConfFromOptions.realTime.getOrElse(false)
  )

  EventSimulator.compute(params)

}
