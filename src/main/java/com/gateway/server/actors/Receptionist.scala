package com.gateway.server.actors

import akka.actor.{ActorLogging, Props, Actor}
import akka.routing.SmallestMailboxRouter
import java.text.MessageFormat
import com.mongodb._
import com.github.nscala_time.time.Imports._
import com.gateway.server.actors.Persistor.{UpdateCompiled, UpdateRecent}
import com.gateway.server.exts.{ScraperUrl, MongoConfig}
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import java.net.URLEncoder
import scala.util.Try

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

  val scrapers = context.actorOf(Props.apply(new WebGetter()).withRouter(SmallestMailboxRouter(5)), name = "WebGetter")
  val persistors = context.actorOf(Props.apply(new Persistor(mongoConfig, 5)).withRouter(SmallestMailboxRouter(2)), name = "Persistor")

  var scheduledUrls = List[String]()
  var scheduledTeamNames = List[String]()
  var teamsResults = List[BasicDBObject]()

  def receive = idle()

  def idle(): Receive = {
    case StartScraper => {
      Try {
        dao.open

        teamNames foreach {
          teamName =>
            val url0 = MessageFormat.format(url, URLEncoder.encode(teamName, "UTF-8"))
            self ! PrependUrl(teamName, url0, dao.lastScrapDt getOrElse (DateTime.now - 10.years))
        }
      } recover {
        case ex: Throwable => {
          log.info("PrependUrl error:" + ex.getMessage);
          self ! ScrapDone
        }
      }
    }

    case PrependUrl(teamName, url, lastScrapDt) => {
      scheduledUrls = url :: scheduledUrls
      scheduledTeamNames = teamName :: scheduledTeamNames
      scrapers ! StartCollect(teamName, url, lastScrapDt)
      if (scheduledTeamNames.size == teamNames.size)
        context.become(running())
    }
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
      log.info(s"Collect result size ${teamsResults.size}")

      if (teamsResults.size > 0) {
        log.info("Save result in db")
        dao.storeResults(teamsResults)
        dao.saveScrapStat(scrapDt.toDate, teamsResults.size)
        dao.updateStanding
      }

      scheduledTeamNames foreach { teamName =>
        persistors ! UpdateRecent(teamName)
      }

      dao.close
    }

    case UpdateCompiled(team, status) => {
      log.info(s"UpdateCompiled  ${team} ${status}")
      scheduledTeamNames = scheduledTeamNames copyWithout (team)

      if (scheduledTeamNames.isEmpty) {
        self ! ScrapDone
      }
    }

    case ScrapDone => {
      log.info(s"ScrapDone ScraperRootActor ${scheduledTeamNames.size} ${scheduledUrls.size}")
      context.become(idle)
    }
  }
}