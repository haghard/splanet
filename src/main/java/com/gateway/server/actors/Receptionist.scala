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
import akka.actor.Status.Failure
import akka.actor.SupervisorStrategy.Restart

object Receptionist {

  case object Done

  case object SaveResults

  case class Go(url: TargetUrl)

  case class Connected(dt: DateTime)

  case class TargetUrl(teamName: String, url: String, lastScrapDt: DateTime)

  case class RemoveResultList(url: String)

  case class TryLater(msgError: String)

  case object UpdateCompiled
}

import WebGetter._
import Receptionist._
import scala.concurrent.duration._
import akka.pattern.pipe

class Receptionist(implicit val bindingModule: BindingModule) extends Actor with ActorLogging with
                                                              Injectable with CollectionImplicits {
  val url = inject[String](ScraperUrl)
  val mongoConfig = inject[MongoConfig]
  val dao = inject[Dao]
  val recentWindow = 5
  var updateBatch = List[BasicDBObject]()

  import context.dispatcher

  //
  Future(dao.open) map { x => Connected(dao.lastScrapDt.getOrElse(DateTime.now - 10.years)) } pipeTo context.parent

  override def postStop() = {
    dao.close
    log.info(s"${self.path} was stopped")
  }

  override def receive = waiting()

  def waiting(): Receive = {
    case Go(targetUrl) => context.become(runNext(List(targetUrl)))
  }

  def dequeueJob(q: List[TargetUrl], map: Map[TargetUrl, List[BasicDBObject]], scrapDt: DateTime): Receive = {
    updateBatch = map.values.head ::: updateBatch
    val copy = q.copyWithout(map.keys.head)
    if (copy.isEmpty) {
      if (!updateBatch.isEmpty) {
        context.actorOf(Props(new BatchPersistor(dao, recentWindow, updateBatch, scrapDt))
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

    case ComebackLater(task) => {
      log.info("lets try scrap later for {}", task.teamName)
      context.system.scheduler.scheduleOnce(new FiniteDuration(10, TimeUnit.SECONDS)) {
        context.actorOf(Props(new WebGetter(task)).withDispatcher("scraper-dispatcher"),
          name = task.teamName.replace(" ", "%20"))
      }
    }

    case ProcessedResults(map, scrapDt) => context.become(dequeueJob(q, map, scrapDt))

    case PersistLater(updateBatch, scrapDt) => {
      log.info("lets try persist later")
      context.system.scheduler.scheduleOnce(new FiniteDuration(30, TimeUnit.SECONDS),
        context.actorOf(Props(new BatchPersistor(dao, recentWindow, updateBatch, scrapDt))
          .withDispatcher("db-dispatcher")), SaveResults)
    }

    case UpdateCompiled => {
      updateBatch = Nil
      context.become(waiting)
      context.parent ! Done
      context.stop(self)
    }
  }

  def runNext(q: List[TargetUrl]): Receive = {
    if(q.isEmpty) waiting()
    else {
      val task = q.head
      context.actorOf(Props(new WebGetter(task))
        .withDispatcher("scraper-dispatcher"), name = task.teamName.replace(" ", "%20"))
      running(q)
    }
  }
}