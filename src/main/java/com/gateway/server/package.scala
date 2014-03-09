package com.gateway.server

import org.vertx.java.core.http.{HttpServerRequest, HttpServerResponse}
import org.vertx.java.core.Handler
import rx.util.functions.{Func2, Func1, Action1}
import java.util.concurrent.atomic.AtomicInteger
import io.vertx.rxcore.java.eventbus.RxMessage
import org.vertx.java.core.json.{JsonArray, JsonObject}
import com.google.common.collect.HashMultiset
import com.escalatesoft.subcut.inject.BindingId

package object exts {

  implicit def fnToHandler[T](f: T => HttpServerResponse): Handler[T] = new Handler[T]() {
    override def handle(req: T) = f(req)
  }

  implicit def fnToHandler1[T](f: T => Any): Handler[T] = new Handler[T]() {
    override def handle(req: T) = f(req)
  }

  implicit def fnToFunc2[R, T](f: (R, T) => R): Func2[R, T, R] = new Func2[R,T,R]() {
    override def call(first: R, second: T) = f(first, second)
  }

  implicit def fnToAction1[T](fn: T => HttpServerResponse): Action1[T] = new Action1[T]() {
    override def call(event: T) = fn(event)
  }

  implicit def fnToAction2[T](fn: T => Unit): Action1[T] = new Action1[T]() {
    override def call(event: T) = fn(event)
  }

  implicit def fnToAction3[T](fn: T => Any): Action1[T] = new Action1[T]() {
    override def call(event: T) = fn(event)
  }

  implicit def fnToFunc1[T, E](fn: T => E): Func1[T, E] = new Func1[T, E]() {
    override def call(event: T) = fn(event)
  }

  implicit def runnableToFunc(f: () => Unit): Runnable = new Runnable {
    def run() = f()
  }

  val MONGO_RESULT_FIELD = "results"

  import scala.collection.JavaConverters._
  def reduceByMetrics = {( json: JsonObject, message: RxMessage[JsonObject] ) =>
    val collection = message.body.getString("collection")
    json getArray(MONGO_RESULT_FIELD) size match {
      case 0 => {
        val arrayIter = message.body.getArray(MONGO_RESULT_FIELD).iterator.asScala
        val returned = for( j <- arrayIter ) yield {
          val jsObject = j.asInstanceOf[JsonObject]
          new JsonObject()
            .putString("team", jsObject.getString("_id"))
            .putNumber(collection, jsObject.getNumber("value"))
        }
        new JsonObject().putArray(MONGO_RESULT_FIELD,
          returned.foldLeft(new JsonArray()) { (acc: JsonArray, cur: JsonObject) => acc.add(cur) })
      }
      case 30 => {
        val messageIter: Iterator[AnyRef] = message.body.getArray(MONGO_RESULT_FIELD).iterator.asScala
        val prevIter: Iterator[AnyRef] = json.getArray(MONGO_RESULT_FIELD).iterator.asScala

        val returned = for { (m, p) <- messageIter zip prevIter } yield {
          val messageObj = m.asInstanceOf[JsonObject]
          val prevObj = p.asInstanceOf[JsonObject]
          prevObj.putNumber(collection, messageObj getNumber("value"))
        }
        new JsonObject().putArray(MONGO_RESULT_FIELD,
          returned.foldLeft(new JsonArray()) { (acc: JsonArray, cur: JsonObject) => acc.add(cur) })
      }
      case other => throw new IllegalArgumentException("Standing size should be equals to 30 but found" + other)
    }
  }

  def reduceAll = { ( json: JsonObject, message: RxMessage[JsonObject] ) =>
    json getArray(MONGO_RESULT_FIELD) size match {
      case 0 => message.body
      case 30 => {
        val newArray = message.body.getArray(MONGO_RESULT_FIELD)
        val curArray = json.getArray(MONGO_RESULT_FIELD)
        val newIter: Iterator[AnyRef] = newArray.iterator.asScala
        val curIter: Iterator[AnyRef] = curArray.iterator.asScala

        val returnArray = for { (n, c) <- newIter zip curIter } yield {
          val newObj = n.asInstanceOf[JsonObject]
          val curObj = c.asInstanceOf[JsonObject]
          new JsonObject()
            .putString("_id", curObj.getString("_id"))
            .putNumber("value", newObj.getNumber("value").intValue + curObj.getNumber("value").intValue)
        }

        new JsonObject().putArray(MONGO_RESULT_FIELD,
          returnArray.foldLeft(new JsonArray()) { (acc: JsonArray, cur: JsonObject) => acc.add(cur) })
      }
      case other => throw new IllegalArgumentException("Standing size should be equals to 30 but found" + other)
    }
  }

  trait MongoResponseFieldParser0 extends ((RxMessage[JsonObject], String) => Option[String]) {
    override def apply(message: RxMessage[JsonObject], fieldName: String): Option[String] = {
      val array = for {
        v <- Option(message.body().getArray(MONGO_RESULT_FIELD))
        if (v.size() > 0)
      } yield {
        v.get(0).asInstanceOf[JsonObject].getArray(fieldName)
      }
      if (array.isDefined) {
        Some(array.get.toArray.mkString(","))
      }
      else {
        None
      }
    }
  }

  trait MongoResponseParser1 extends (RxMessage[JsonObject] => Option[String]) {
    override def apply(message: RxMessage[JsonObject]): Option[String] = {
      for {
        v <- Option(message.body().getArray(MONGO_RESULT_FIELD));
        if (v.size() > 0)
      } yield {
        v.toString.replaceAll("\\[|]", "")
      }
    }
  }

  trait MongoResponseFieldParser2 extends ((RxMessage[JsonObject], String) => Option[JsonArray]) {
    override def apply(message: RxMessage[JsonObject], fieldName: String): Option[JsonArray] = {
      for {
        v <- Option(message.body().getArray(MONGO_RESULT_FIELD))
        if (v.size() > 0)
      } yield {
        v.get(0).asInstanceOf[JsonObject].getArray(fieldName)
      }
    }
  }

  trait MongoResponseStatProcessor extends ((RxMessage[JsonObject], String) => Option[JsonObject]) {
    override def apply(message: RxMessage[JsonObject], teamName: String): Option[JsonObject] = {
      for {
        v <- Option(message.body().getArray(MONGO_RESULT_FIELD))
        if (v.size > 0)
      } yield {
        import scala.collection.JavaConversions.asScalaIterator
        val recentResults: Iterator[AnyRef] = v.iterator

        val stats = HashMultiset.create[String]()
        recentResults foreach {
          result =>
            val r = result.asInstanceOf[JsonObject]
            if (r.getNumber("homeScore").intValue() > r.getNumber("awayScore").intValue()) {
              stats.add(r.getString("homeTeam"))
            } else {
              stats.add(r.getString("awayTeam"))
            }
        }
        val win = stats.count(teamName)
        val lose = stats.size - win
        new JsonObject()
          .putString("team", teamName).putString("results", Array(win, lose).mkString("-"))
      }
    }
  }

  object ResponseFieldParser extends MongoResponseFieldParser0

  object ResponseParser extends MongoResponseParser1

  object ResponseFieldParserToArray extends MongoResponseFieldParser2

  object RecentHealthProcessor extends MongoResponseStatProcessor

  trait ResponseWriter {
    def write(line: String)
  }

  case class MongoConfig(ip: String, port: Int, db: String, username:String, password: String)

  class ChunkedResponseWriter(req: HttpServerRequest, threshold: AtomicInteger,
                              currentChuckNumber: AtomicInteger = new AtomicInteger) extends ResponseWriter {

    def write(line: String): Unit = currentChuckNumber.incrementAndGet() match {
      case n => {
        if (n == 1) {
          req.response().write("{ \"results\": [" + line)
        }
        else if (n > 1) {
          req.response().write("," + line)
        }

        if (n == threshold.get()) {
          req.response().end("] }")
        }
      }
    }
  }

  class DBAccessException(msg: String, th: Throwable) extends Exception(msg, th)

  case class IllegalHttpReqParams(msg: String) extends Exception(msg)
  case class InvalidAuth(msg: String) extends Exception(msg)

  object DBAccessException {
    def create(msg: String) = new DBAccessException(msg, null)

    def create(msg: String, cause: Throwable) = new DBAccessException(msg, cause)
  }

  object MongoPersistorKey extends BindingId

  object MongoResponseArrayKey extends BindingId

  object WebRootKey extends BindingId

  object MongoHost extends BindingId

  object MongoPort extends BindingId

  object MongoDBName extends BindingId

  object HttpServerPort extends BindingId

  object ScraperUrl extends BindingId

  object ScraperStatCollectionKey extends BindingId

  object RecentCollectionKey extends BindingId

  object ScraperPeriod extends BindingId

  object ScraperDelay extends BindingId
}