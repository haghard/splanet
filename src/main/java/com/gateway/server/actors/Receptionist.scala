package com.gateway.server.actors

import akka.actor._
import com.mongodb._
import scala.concurrent.Future
import java.util.concurrent.TimeUnit
import com.github.nscala_time.time.Imports._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import scala.collection.immutable.{TreeSet, Map}
import java.net.URLEncoder

object Receptionist {

  case object Done

  case object SaveResults

  case object UpdateCompiled

  case class Go(url: TargetUrl, stages: TreeSet[Stage])

  case class Connected(dt: DateTime, stages: TreeSet[Stage])

  //it's did't used as message
  case class Stage(start: DateTime, end:DateTime, name: String)

  case class RemoveResultList(url: String)

  case class TryLater(msgError: String)

  case class TargetUrl(teamName: String, url: String, startScrapDt: DateTime)
}

import com.gateway.server.actors.WebGetter._
import Receptionist._
import scala.concurrent.duration._
import akka.pattern.pipe

class Receptionist(implicit val bindingModule: BindingModule) extends Actor with ActorLogging with
                                                              Injectable with com.gateway.server.actors.CollectionImplicits {
  private val teamNames = inject[List[String]]
  private val dao = inject[Dao]
  private val recentWindow = 5
  private var updateBatch = List[BasicDBObject]()

  import com.gateway.server.exts.stageOrdering
  private var stages = TreeSet[Stage]()

  implicit val exc = context.system.dispatchers.lookup("db-dispatcher")
  Future(dao.open).map({ x => Connected(
          dao.lastScrapDt.getOrElse(DateTime.now - 10.years),
          dao.stages
          )
  }) pipeTo context.parent

  override def receive = waiting()

  def waiting(): Receive = {
    case Go(targetUrl, stages) => {
      this.stages = stages
      context.become(runNext(List(targetUrl)))
    }
  }

  def dequeueJob(q: List[TargetUrl], map: Map[TargetUrl, List[BasicDBObject]], scrapDt: DateTime): Receive = {
    updateBatch = map.values.head ::: updateBatch
    val copy = q.copyWithout(map.keys.head)
    if (copy.isEmpty) {
      if (!updateBatch.isEmpty) {
        context.actorOf(BatchPersistor(dao, recentWindow, updateBatch, scrapDt, teamNames)) ! SaveResults
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
    case Go(targetUrl, _) => context.become(enqueueJob(q, targetUrl))

    case GameResults(map, scrapDt) => context.become(dequeueJob(q, map, scrapDt))

    case ScrapLater(task) => {
      context.system.scheduler.scheduleOnce(
        new FiniteDuration(30, TimeUnit.SECONDS))({
        context.actorOf(WebGetter(task, stages), name = URLEncoder.encode(task.teamName, "UTF-8"))
      })(context.system.dispatchers.lookup("scraper-dispatcher"))
    }

    case PersistLater(updateBatch, scrapDt) => {
      context.system.scheduler.scheduleOnce(
        new FiniteDuration(30, TimeUnit.SECONDS),
        context.actorOf(BatchPersistor(dao, recentWindow, updateBatch, scrapDt, teamNames)),
        SaveResults)(context.system.dispatchers.lookup("db-dispatcher"))
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
      context.actorOf(WebGetter(task, stages), name = task.teamName.replace(" ", "%20"))
      running(q)
    }
  }
}