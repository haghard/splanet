package com.gateway.server.actors

import akka.actor._
import java.text.MessageFormat
import com.mongodb._
import com.github.nscala_time.time.Imports._
import com.gateway.server.actors.BatchPersistor.{UpdateRecentBatch, UpdateCompiled}
import com.gateway.server.exts.{ScraperUrl, MongoConfig}
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import java.net.URLEncoder
import com.gateway.server.actors.BatchPersistor.UpdateCompiled
import com.gateway.server.exts.MongoConfig
import com.gateway.server.actors.BatchPersistor.UpdateRecentBatch
import scala.util.Success
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

object Receptionist {

  case object Go

  case object Done

  case object Connected

  case class SaveResults(scrapDt: DateTime)

  case class PrependUrl(teamName: String, url: String, lastScrapDt: DateTime)

  case class RemoveResultList(url: String)

  case class TryLater(msgError: String)
}

import WebGetter._
import Receptionist._

class Receptionist(implicit val bindingModule: BindingModule) extends Actor with ActorLogging with Injectable with CollectionImplicits {
  val teamNames = inject[List[String]]
  val url = inject[String](ScraperUrl)
  val mongoConfig = inject[MongoConfig]
  val dao = inject[Dao]
  val persistorsNumber = 2

  var scheduledUrls = List[String]()
  var scheduledTeamNames = List[String]()
  var teamsResults = List[BasicDBObject]()

  import context.dispatcher
  import scala.concurrent.duration._
  import akka.pattern.pipe

  Future { dao.open }.map(x => Connected) pipeTo context.parent

  override def receive = idle()

  override def postStop() {
    log.info(s"${self.path} was stopped")
  }

  def idle(): Receive = {    
    case Go => {
      teamNames foreach { teamName =>
          self ! PrependUrl(teamName, MessageFormat.format(url, URLEncoder.encode(teamName, "UTF-8")),
            dao.lastScrapDt getOrElse (DateTime.now - 10.years))
      }
    }

    case PrependUrl(teamName, url, lastScrapDt) => {
      scheduledUrls = url :: scheduledUrls
      scheduledTeamNames = teamName :: scheduledTeamNames
      context.actorOf(Props(new WebGetter(teamName, url, lastScrapDt)).withDispatcher("scraper-dispatcher"),
        name = teamName.replace(" ", "%20"))
      if (scheduledTeamNames.size == teamNames.size)
        context.become(running())
    }
  }

  private def scheduleUpdateRecent(scheduledTeamName: List[String], batchSize: Int) = {
    def loop(lists: (List[String], List[String])): Unit = lists._1 match {
      case Nil => {}
      case lst => {
        val persistor = context.actorOf(Props(new BatchPersistor(5)).withDispatcher("db-dispatcher"))
        persistor ! UpdateRecentBatch(lists._1)
        loop(lists._2.splitAt(batchSize))
      }
    }

    loop(scheduledTeamName.splitAt(batchSize))
  }

  def running(): Receive = {
    case ComebackLater(teamName, url, lastScrapDt) => {
      context.system.scheduler.scheduleOnce(new FiniteDuration(10, TimeUnit.SECONDS)) {
        context.actorOf(Props(new WebGetter(teamName, url, lastScrapDt)).withDispatcher("scraper-dispatcher"),
          name = teamName.replace(" ", "%20"))
      }
    }

    case ProcessedResults(map, scrapDt) => {
      if (map.values.head != Nil) {
        teamsResults = map.values.head ::: teamsResults
      }

      scheduledUrls = scheduledUrls copyWithout (map.keys.head)

      if (scheduledUrls.isEmpty) {
        self ! SaveResults(scrapDt)
      }
    }

    case SaveResults(scrapDt: DateTime) => {
      if (teamsResults.size > 0) {
        log.info("Results size: {} ", teamsResults.size)
        
        dao.persist(teamsResults, scrapDt.toDate, teamsResults.size) match {
          case Success(r) => {
            dao.updateStanding
            scheduleUpdateRecent(scheduledTeamNames, scheduledTeamNames.size/persistorsNumber)
          }

          case scala.util.Failure(ex) => { log.info(ex.getMessage); self ! Done }
        }
      } else {
        self ! Done
      }
    }

    case UpdateCompiled(team, status) => {
      scheduledTeamNames = scheduledTeamNames copyWithout (team)
      if (scheduledTeamNames.isEmpty) { self ! Done }
    }

    case Done => {
      scheduledUrls = Nil
      scheduledTeamNames = Nil
      teamsResults = Nil
      dao.close

      context.become(idle)
      context.parent ! Done
      context.stop(self)
    }
  }
}