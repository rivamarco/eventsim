package com.interana.eventsim

import java.io.{OutputStream, Serializable}
import java.time.{LocalDateTime, ZoneOffset}

import com.fasterxml.jackson.core.{JsonEncoding, JsonFactory}
import com.interana.eventsim.config.ConfigFromFile
import io.radicalbit.nsdb.api.scala.Bit

import scala.util.parsing.json.JSONObject

class User(val alpha: Double,
           val beta: Double,
           val startTime: LocalDateTime,
           val initialSessionStates: scala.collection.Map[(String,String),WeightedRandomThingGenerator[State]],
           val auth: String,
           val props: Map[String,Any],
           var device: scala.collection.immutable.Map[String,Any],
           val initialLevel: String,
          //fixme not completely convinced about the need of the stream as a constructor parameters
           val stream: OutputStream
          ) extends Serializable with Ordered[User] {

  val userId = Counters.nextUserId
  var session = new Session(
    Some(Session.pickFirstTimeStamp(startTime, alpha, beta)),
      alpha, beta, initialSessionStates, auth, initialLevel)

  override def compare(that: User) =
    (that.session.nextEventTimeStamp, this.session.nextEventTimeStamp) match {
      case (None, None) => 0
      case (_: Some[LocalDateTime], None) => -1
      case (None, _: Some[LocalDateTime]) => 1
      case (thatValue: Some[LocalDateTime], thisValue: Some[LocalDateTime]) =>
        thatValue.get.compareTo(thisValue.get)
    }

  def nextEvent(): Unit = nextEvent(0.0)

  def nextEvent(prAttrition: Double) = {
    session.incrementEvent()
    if (session.done) {
      if (TimeUtilities.rng.nextDouble() < prAttrition ||
          session.currentState.auth == ConfigFromFile.churnedState.getOrElse("")) {
        session.nextEventTimeStamp = None
        // TODO: mark as churned
      }
      else {
        session = session.nextSession
      }
    }
  }

  private val EMPTY_MAP = Map()

  def eventString = {
    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    var m = device.+(
      "ts" -> session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC).toEpochMilli,
      "userId" -> (if (showUserDetails) userId else ""),
    )

    if (showUserDetails)
      m ++= props

    /* most of the event generator code is pretty generic, but this is hard-coded
     * for a fake music web site
     */
    if (session.currentState.page=="NextSong")
      m += (
        "artist" -> session.currentSong.get._2,
        "song" -> session.currentSong.get._3,
        "length" -> session.currentSong.get._4
        )

    val j = new JSONObject(m)
    j.toString()
  }


  val writer = User.jsonFactory.createGenerator(stream, JsonEncoding.UTF8)

  /* fixme
      the design of this class must be updated in order to implement an agnostic abstraction for writers
      NSDb for example does not suite the outputstream paradigm
   */
  def writeEvent() = {
    // use Jackson streaming to maximize efficiency
    // (earlier versions used Scala's std JSON generators, but they were slow)
    val showUserDetails = ConfigFromFile.showUserWithState(session.currentState.auth)
    writer.writeStartObject()
    writer.writeNumberField("ts", session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC)toEpochMilli())
    writer.writeStringField("userId", if (showUserDetails) userId.toString else "")
    writer.writeStringField("level", session.currentState.level)
    if (showUserDetails) {
      props.foreach((p: (String, Any)) => {
        p._2 match {
          case _: Long => writer.writeNumberField(p._1, p._2.asInstanceOf[Long])
          case _: Int => writer.writeNumberField(p._1, p._2.asInstanceOf[Int])
          case _: Double => writer.writeNumberField(p._1, p._2.asInstanceOf[Double])
          case _: Float => writer.writeNumberField(p._1, p._2.asInstanceOf[Float])
          case _: String => writer.writeStringField(p._1, p._2.asInstanceOf[String])
        }})
    }
    if (session.currentState.page=="NextSong") {
      writer.writeStringField("artist", session.currentSong.get._2)
      writer.writeStringField("song",  session.currentSong.get._3)
      writer.writeNumberField("length", session.currentSong.get._4)
    }
    writer.writeEndObject()
    writer.writeRaw('\n')
    writer.flush()
  }

  def tsToString(ts: LocalDateTime) = ts.toString()

  def nextEventTimeStampString =
    tsToString(this.session.nextEventTimeStamp.get)

  def mkString = props.+(
    "alpha" -> alpha,
    "beta" -> beta,
    "startTime" -> tsToString(startTime),
    "initialSessionStates" -> initialSessionStates,
    "nextEventTimeStamp" -> tsToString(session.nextEventTimeStamp.get) ,
    "sessionId" -> session.sessionId ,
    "userId" -> userId ,
    "currentState" -> session.currentState)
}

object User {
  protected val jsonFactory = new JsonFactory()
  jsonFactory.setRootValueSeparator("")

  val nsdbConverter: (User, Bit) => Bit = (u: User, b: Bit) => {

    val showUserDetails = ConfigFromFile.showUserWithState(u.session.currentState.auth)

    var result = b.timestamp(u.session.nextEventTimeStamp.get.toInstant(ZoneOffset.UTC)toEpochMilli())
      .tag("userId", if (showUserDetails) u.userId.toString else "")
      .tag("level", u.session.currentState.level)

    if (showUserDetails) {
      u.props.foreach((p: (String, Any)) => {
        p._2 match {
          case _: Long => result = result.tag(p._1, p._2.asInstanceOf[Long])
          case _: Int => result = result.tag(p._1, p._2.asInstanceOf[Int])
          case _: Double => result = result.tag(p._1, p._2.asInstanceOf[Double])
          case _: Float => result = result.tag(p._1, p._2.asInstanceOf[Float])
          case _: String => result = result.tag(p._1, p._2.asInstanceOf[String])
        }})
    }

    if (u.session.currentState.page=="NextSong") {
      result = result
        .tag("artist", u.session.currentSong.get._2)
        .tag("song",  u.session.currentSong.get._3)
        //length
        .value(u.session.currentSong.get._4)
    } else {
      //length
      result = result.value(0.0)
    }

    if (result.value.isEmpty)
      println("stocazzo...")

    result
  }
}
