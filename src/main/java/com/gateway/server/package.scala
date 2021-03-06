package com.gateway.server

import org.vertx.java.core.http.{HttpServerRequest, HttpServerResponse}
import org.vertx.java.core.Handler
import rx.functions.{Func2, Func1, Action1}
import java.util.concurrent.atomic.AtomicInteger
import io.vertx.rxcore.java.eventbus.RxMessage
import org.vertx.java.core.json.{JsonObject, JsonArray}
import com.google.common.collect.HashMultiset
import com.escalatesoft.subcut.inject.BindingId
import com.gateway.server.actors.Receptionist.Stage
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import java.util.concurrent.{ AbstractExecutorService, TimeUnit }
import java.util.Collections
import rx.lang.scala.Observable

package object exts {

  object ImplicitFunctionConversions {

    implicit def fnToHandler[T](f: T => HttpServerResponse): Handler[T] = new Handler[T]() {
      override def handle(req: T) = f(req)
    }

    implicit def fnToHandler1[T](f: T => Any): Handler[T] = new Handler[T]() {
      override def handle(req: T) = f(req)
    }

    implicit def fnToFunc2[R, T](f: (R, T) => R): Func2[R, T, R] = new Func2[R, T, R]() {
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

  def mergePlayoffResults(homeResults: JsonObject, awayResults: JsonObject): JsonObject = {
    (homeResults.getArray(MONGO_RESULT_FIELD).size, awayResults.getArray(MONGO_RESULT_FIELD).size()) match {
      case (16,16) => {
        val homeArray = homeResults.getArray(MONGO_RESULT_FIELD).iterator.asScala
        val awayArray = awayResults.getArray(MONGO_RESULT_FIELD).iterator.asScala
        val table = for { (l, r) <- homeArray zip awayArray } yield {
          val lObj = l.asInstanceOf[JsonObject]
          val rObj = r.asInstanceOf[JsonObject]
          new JsonObject()
            .putString("team", lObj.getString("team"))
            .putNumber("w", lObj.getNumber("w").intValue() + rObj.getNumber("w").intValue())
            .putNumber("l", lObj.getNumber("l").intValue() + rObj.getNumber("l").intValue())
        }
        new JsonObject().putArray(MONGO_RESULT_FIELD,
          table.foldLeft(new JsonArray()) { (acc: JsonArray, cur: JsonObject) => acc.add(cur) })
      }

      case other =>
        throw new IllegalArgumentException("Playoff size should be equals to (16,16) but found" + other)
    }
  }

  def reduceAll = { ( json: JsonObject, message: RxMessage[JsonObject] ) =>
    json.getArray(MONGO_RESULT_FIELD) size match {
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
        recentResults foreach { result =>
          val r = result.asInstanceOf[JsonObject]
          if (r.getNumber("homeScore").intValue() > r.getNumber("awayScore").intValue()) {
            stats.add(r.getString("homeTeam"))
          } else {
            stats.add(r.getString("awayTeam"))
          }
        }
        val win = stats.count(teamName)
        val lose = stats.size - win
        new JsonObject().putObject("results", new JsonObject().putString("team", teamName)
          .putString("results", Array(win, lose).mkString("-")))
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
                              val currentChuckNumber: AtomicInteger = new AtomicInteger) extends ResponseWriter {

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

  object SettingCollectionKey extends BindingId

  object ScraperPeriod extends BindingId

  object ScraperDelay extends BindingId

  case class Principal(email: String, password: String)


  object ExecutionContextExecutorServiceBridge {
    def apply(ec: ExecutionContext): ExecutionContextExecutorService = ec match {
      case null => throw null
      case eces: ExecutionContextExecutorService => eces
      case other => new AbstractExecutorService with ExecutionContextExecutorService {
        override def prepare(): ExecutionContext = other
        override def isShutdown = false
        override def isTerminated = false
        override def shutdown() = ()
        override def shutdownNow() = Collections.emptyList[Runnable]
        override def execute(runnable: Runnable): Unit = other execute runnable
        override def reportFailure(t: Throwable): Unit = other reportFailure t
        override def awaitTermination(length: Long,unit: TimeUnit): Boolean = false
      }
    }
  }

  private [server] implicit val stageOrdering = new Ordering[Stage] {
    override def compare(x: Stage, y: Stage) = x.end.compareTo(y.end)
  }

  private [server] val regularEx = """(regular-)(\d{4}/\d{4})""".r
  private [server] val playoffEx = """(playoff-)(\d{4}/\d{4})""".r


  case class Bool(b: Boolean) {
    def ?[X](t: => X) = new {
      def |(f: => X) = if(b) t else f
    }
  }

  implicit def BooleanBool(b: Boolean) = Bool(b)



  trait ObservableTaskBuilder {

    def apply(f: HttpServerRequest => JsonObject) = new ObservableTask {
      def run(request: HttpServerRequest) =
        invokeBlock(request, f).subscribe({ json =>
          request.response().end(json.toString)
        }, { ex =>
          request.response().end(new JsonObject().putString("error", ex.getMessage).toString)
        })
    }

    protected def invokeBlock(request: HttpServerRequest, f: HttpServerRequest => JsonObject): Observable[JsonObject]

  }

  /**
   *
   *
   */
  trait ObservableTask {
    def run(request: HttpServerRequest)
  }

  object ObservableTask extends ObservableTaskBuilder {

    def invokeBlock(request: HttpServerRequest, f: HttpServerRequest => JsonObject): Observable[JsonObject] =
      Observable.items(f(request))
  }

  object AuthenticatedObservableTask extends ObservableTaskBuilder {
    def invokeBlock(request: HttpServerRequest, f: HttpServerRequest => JsonObject): Observable[JsonObject] = {
      Observable.items(Option(request.headers.get("Authorization"))).map({ auth: Option[String] =>
        auth match {
          case Some(r) => f(request)
          case None => new JsonObject().putString("msg", "no auth")
        }
      })
    }
  }
}