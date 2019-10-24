package com.interana.eventsim

import java.time.LocalDateTime

import com.interana.eventsim.buildin.RandomSongGenerator
import com.interana.eventsim.config.ConfigFromFile

/**
  * Object to capture session related calculations and properties
  */
class Session(val seed: Long,
              val firstUserId: Int,
              var nextEventTimeStamp: Option[LocalDateTime],
              val alpha: Double, // expected request inter-arrival time
              val beta: Double, // expected session inter-arrival time
              val initialStates: scala.collection.Map[(String, String), WeightedRandomThingGenerator[State]],
              val auth: String,
              val level: String) {

  val sessionId           = new Counters(firstUserId).nextSessionId
  var itemInSession       = 0
  var done                = false
  var currentState: State = initialStates((auth, level)).randomThing(seed)
//  val randomSongGenerator = new RandomSongGenerator((seed))
  var currentSong: Option[(String, String, String, Double)] =
    if (currentState.page == "NextSong") Some(RandomSongGenerator.nextSong(seed)) else None
  var currentSongEnd: Option[LocalDateTime] =
    if (currentState.page == "NextSong") Some(nextEventTimeStamp.get.plusSeconds(currentSong.get._4.toInt)) else None

  val timeUtilities = new TimeUtilities(seed)

  def incrementEvent() = {
    val nextState = currentState.nextState(timeUtilities.rng)
    nextState match {
      case None =>
        done = true
      case x if 300 until 399 contains x.get.status =>
        nextEventTimeStamp = Some(nextEventTimeStamp.get.plusSeconds(1))
        currentState = nextState.get
        itemInSession += 1

      case x if x.get.page == "NextSong" =>
        if (currentSong.isEmpty) {
          nextEventTimeStamp = Some(
            nextEventTimeStamp.get.plusSeconds(timeUtilities.exponentialRandomValue(alpha).toInt))
          currentSong = Some(RandomSongGenerator.nextSong(seed))
        } else if (nextEventTimeStamp.get.isBefore(currentSongEnd.get)) {
          nextEventTimeStamp = currentSongEnd
          currentSong = Some(RandomSongGenerator.nextSong(seed, currentSong.get._1))
        } else {
          nextEventTimeStamp = Some(
            nextEventTimeStamp.get.plusSeconds(timeUtilities.exponentialRandomValue(alpha).toInt))
          currentSong = Some(RandomSongGenerator.nextSong(seed, currentSong.get._1))
        }
        currentSongEnd = Some(nextEventTimeStamp.get.plusSeconds(currentSong.get._4.toInt))
        currentState = nextState.get
        itemInSession += 1

      case _ =>
        nextEventTimeStamp = Some(nextEventTimeStamp.get.plusSeconds(timeUtilities.exponentialRandomValue(alpha).toInt))
        currentState = nextState.get
        itemInSession += 1

    }
  }

  def nextSession(seed: Long, firstUserId: Int) =
    new Session(
      seed,
      firstUserId,
      Some(Session.pickNextSessionStartTime(seed, nextEventTimeStamp.get, beta)),
      alpha,
      beta,
      initialStates,
      currentState.auth,
      currentState.level
    )

}

object Session {

  def pickFirstTimeStamp(seed: Long,
                         st: LocalDateTime,
                         alpha: Double, // expected request inter-arrival time
                         beta: Double // expected session inter-arrival time
  ): LocalDateTime = {
    // pick random start point, iterate to steady state
    val startPoint = st.minusSeconds(beta.toInt * 2)
    var candidate  = pickNextSessionStartTime(seed, startPoint, beta)
    while (candidate.isBefore(st.minusSeconds(beta.toInt))) {
      candidate = pickNextSessionStartTime(seed, candidate, beta)
    }
    candidate
  }

  def pickNextSessionStartTime(seed: Long, lastTimeStamp: LocalDateTime, beta: Double): LocalDateTime = {
    val timeUtilities                = new TimeUtilities(seed)
    val randomGap                    = timeUtilities.exponentialRandomValue(beta).toInt + ConfigFromFile.sessionGap
    val nextTimestamp: LocalDateTime = timeUtilities.standardWarp(lastTimeStamp.plusSeconds(randomGap))
    assert(randomGap > 0)

    if (nextTimestamp.isBefore(lastTimeStamp)) {
      // force forward progress
      pickNextSessionStartTime(seed, lastTimeStamp.plusSeconds(ConfigFromFile.sessionGap), beta)
    } else if (timeUtilities.keepThisDate(lastTimeStamp, nextTimestamp)) {
      nextTimestamp
    } else
      pickNextSessionStartTime(seed, nextTimestamp, beta)
  }
}
