package com.gateway.server.actors

import akka.actor._
import com.mongodb._
import scala.concurrent.Future
import java.util.concurrent.TimeUnit
import com.gateway.server.exts.ScraperUrl
import com.gateway.server.exts.MongoConfig
import com.github.nscala_time.time.Imports._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import scala.collection.immutable.Map

object Receptionist {

  case object Done

  case object SaveResults

  case object UpdateCompiled

  case class Go(url: TargetUrl)

  case class Connected(dt: DateTime)

  case class RemoveResultList(url: String)

  case class TryLater(msgError: String)

  case class TargetUrl(teamName: String, url: String, lastScrapDt: DateTime)
}

import WebGetter._
import Receptionist._
import scala.concurrent.duration._
import akka.pattern.pipe

class Receptionist(implicit val bindingModule: BindingModule) extends Actor with ActorLogging with
                                                              Injectable with CollectionImplicits {
  val teamNames = inject[List[String]]
  val url = inject[String](ScraperUrl)
  val mongoConfig = inject[MongoConfig]
  val dao = inject[Dao]
  val recentWindow = 5
  var updateBatch = List[BasicDBObject]()

  import context.dispatcher

  Future(dao.open) map { x => Connected(dao.lastScrapDt.getOrElse(DateTime.now - 10.years)) } pipeTo context.parent

  override def receive = waiting()

  def waiting(): Receive = {
    case Go(targetUrl) => context.become(runNext(List(targetUrl)))
  }

  def dequeueJob(q: List[TargetUrl], map: Map[TargetUrl, List[BasicDBObject]], scrapDt: DateTime): Receive = {
    updateBatch = map.values.head ::: updateBatch
    val copy = q.copyWithout(map.keys.head)
    if (copy.isEmpty) {
      if (!updateBatch.isEmpty) {
        context.actorOf(BatchPersistor(dao, recentWindow, updateBatch, scrapDt, teamNames)
          .withDispatcher("db-dispatcher")) ! SaveResults
      } else {
        self ! UpdateCompiled
      }
    }

    running(copy)
  }

  /**
   * Allow to do throttling
   * @param q
   * @param task
   * @return
   */
  def enqueueJob(q: List[TargetUrl], task: TargetUrl): Receive = {
    if ((q.size) > 35) {
      log.debug("Queue overflow. Ignore task {}", task.teamName)
      running(q)
    } else runNext(q.+:(task))
  }

  def running(q: List[TargetUrl]): Receive = {
    case Go(targetUrl) => context.become(enqueueJob(q, targetUrl))

    case ProcessedResults(map, scrapDt) => context.become(dequeueJob(q, map, scrapDt))

    case ScrapLater(task) => {
      implicit val disp = context.system.dispatchers.lookup("scraper-dispatcher")
      context.system.scheduler.scheduleOnce(new FiniteDuration(10, TimeUnit.SECONDS)) {
        context.actorOf(WebGetter(task).withDispatcher("scraper-dispatcher"),
          name = task.teamName.replace(" ", "%20"))
      }
    }

    case PersistLater(updateBatch, scrapDt) => {
      implicit val disp = context.system.dispatchers.lookup("scraper-dispatcher")
      context.system.scheduler.scheduleOnce(new FiniteDuration(30, TimeUnit.SECONDS),
        context.actorOf(BatchPersistor(dao, recentWindow, updateBatch, scrapDt, teamNames)
          .withDispatcher("db-dispatcher")), SaveResults)
    }

    case UpdateCompiled => {
      dao.close
      updateBatch = Nil
      context.become(waiting)
      context.parent ! Done
      context.stop(self)
    }
  }

  /**
   * Create new WebGetter
   * @param q
   * @return
   */
  def runNext(q: List[TargetUrl]): Receive = {
    if(q.isEmpty) waiting()
    else {
      val task = q.head
      context.actorOf(WebGetter(task)
        .withDispatcher("scraper-dispatcher"), name = task.teamName.replace(" ", "%20"))
      running(q)
    }
  }
}