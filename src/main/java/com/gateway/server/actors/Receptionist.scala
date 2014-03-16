package com.gateway.server.actors

import akka.actor.{ActorLogging, Props, Actor}
import akka.routing.SmallestMailboxRouter
import java.text.MessageFormat
import com.mongodb._
import com.github.nscala_time.time.Imports._
import com.gateway.server.actors.BatchPersistor.{UpdateRecentBatch, UpdateCompiled}
import com.gateway.server.exts.{ScraperUrl, MongoConfig}
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import java.net.URLEncoder
import scala.util.{Success, Try}

object Receptionist {

  case object StartScraper

  case object ScrapDone

  case class SaveResults(scrapDt: DateTime)

  case class PrependUrl(teamName: String, url: String, lastScrapDt: DateTime)

  case class RemoveResultList(url: String)

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

  def receive = idle()

  def idle(): Receive = {
    case StartScraper => {
      Try {
        dao.open
        teamNames foreach { teamName =>
          self ! PrependUrl(teamName, MessageFormat.format(url, URLEncoder.encode(teamName, "UTF-8")),
            dao.lastScrapDt getOrElse (DateTime.now - 10.years))
        }
      } recover {
        case ex: Exception => {
          log.info("Dao open error: {}", ex.getMessage)
          dao.close
          self ! ScrapDone
        }
      }
    }

    case PrependUrl(teamName, url, lastScrapDt) => {
      scheduledUrls = url :: scheduledUrls
      scheduledTeamNames = teamName :: scheduledTeamNames
      val getter = context.actorOf(Props(new WebGetter).withDispatcher("scraper-dispatcher"),
        name = teamName.replace(" ", "%20"))
      getter ! StartCollect(teamName, url, lastScrapDt)
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

          case scala.util.Failure(ex) => { log.info(ex.getMessage); self ! ScrapDone }
        }
      } else {
        self ! ScrapDone
      }
      dao.close
    }

    case UpdateCompiled(team, status) => {
      //log.info("UpdateCompiled {} {} ", team, status)
      scheduledTeamNames = scheduledTeamNames copyWithout (team)

      if (scheduledTeamNames.isEmpty) {
        self ! ScrapDone
      }
    }

    case ScrapDone => {
      scheduledUrls = Nil
      scheduledTeamNames = Nil
      teamsResults = Nil
      context.become(idle)
      log.info("Receptionist become idle")
    }
  }
}