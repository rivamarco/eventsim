package com.interana.eventsim

import io.radicalbit.nsdb.api.scala.{Bit, NSDB}
import io.radicalbit.nsdb.rpc.response.RPCInsertResult

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class NSDbWriter[T](val host: String, val port: Int, db: String, namespace: String, metric: String)(
    implicit converter: (T, Bit) => Bit) {

  implicit val ec = ExecutionContext.global

  private val connection = Await.result(NSDB.connect(host, port)(ExecutionContext.global), 10.seconds)

  private val internalMetric: Bit = connection.db(db).namespace(namespace).metric(metric)

  def write(element: T): RPCInsertResult =
    Await.result(connection.write(converter(element, internalMetric)), 10.seconds)
}
