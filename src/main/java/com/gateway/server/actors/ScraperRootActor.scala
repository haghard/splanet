package com.gateway.server.actors

import akka.actor.{Props, ActorLogging, Actor}
import akka.routing.SmallestMailboxRouter
import java.text.MessageFormat
import scala.collection.immutable.Map
import com.mongodb._
import com.github.nscala_time.time.Imports._
import scala.Some
import java.util.Date
import com.gateway.server.actors.RecentActor.{UpdateCompiled, UpdateRecent}
import com.gateway.server.exts.{ScraperStatCollection, ScraperUrl, MongoConfig}
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import java.net.URLEncoder

object ScraperRootActor {

  case object StartScraper

  case object ScrapDone

  case class SaveResults(scrapDt: DateTime)

  case class PrependUrl(teamName: String, url: String, lastScrapDt: DateTime)

  case class RemoveResultList(url: String)

  val homeWinMap = "function () { if (this.homeScore > this.awayScore) emit( this.homeTeam, 1 ); }"
  val awayWinMap = "function () { if (this.homeScore < this.awayScore) emit( this.awayTeam, 1 ); }"
  val homeLoseMap = "function () { if (this.homeScore < this.awayScore) emit( this.homeTeam, 1 ); }"
  val awayLoseMap = "function () { if (this.homeScore > this.awayScore) emit( this.awayTeam, 1 ); }"

  val reduce = "function (key, values) { return Array.sum(values) }"

  val standingMeasurement = Map("homeWin" -> homeWinMap, "homeLose" -> homeLoseMap, "awayWin" -> awayWinMap, "awayLose" -> awayLoseMap)
  
}

/**
 * Collect only home teams to await duplicate
 */
import ScraperActor._
import ScraperRootActor._
final class ScraperRootActor(implicit val bindingModule: BindingModule) extends Actor with Injectable with ActorLogging with CollectionImplicits {

  val teamNames = inject [List[String]]
  val url = inject [String] (ScraperUrl)
  val statTableName = inject [String] (ScraperStatCollection)
  val mongoConfig = inject [MongoConfig]

  val scrapers = context.actorOf(Props.apply(new ScraperActor(self)).withRouter(SmallestMailboxRouter(5)), name = "ScraperActor")
  val recents = context.actorOf(Props.apply(new RecentActor(self, mongoConfig, 5)).withRouter(SmallestMailboxRouter(2)), name = "RecentActor")

  var scheduledUrls = List[String]()
  var scheduledTeamNames = List[String]()

  var teamsResults = List[BasicDBObject]()
  var targetCollection: DBCollection = _
  var db: com.mongodb.DB = _
  var mongoClient: MongoClient = _

  private def findLastScrapDt(db: DB, collectionName: String): Option[DateTime] = {
    import scala.collection.JavaConversions._
    val result = for {
      statCollectionName <- db.getCollectionNames
      if (statCollectionName == collectionName)
    } yield {
      val statColl = db getCollection(statCollectionName)
      val cursor = statColl.find().sort(
        BasicDBObjectBuilder.start("scrapDt", -1).get)
      if (cursor.hasNext) {
        val statObject = cursor.next
        Some(new DateTime(statObject.get("scrapDt").asInstanceOf[Date]))
      } else {
        None
      }
    }

    result.flatten.lastOption
  }

  def receive = {
    case StartScraper => {
      try {
        mongoClient = new MongoClient(mongoConfig.ip, mongoConfig.port)
        db = mongoClient getDB(mongoConfig.db)
        targetCollection = db getCollection("results")
        val lastScrapDt: Option[DateTime] = findLastScrapDt(db, statTableName)

        teamNames foreach { teamName =>
          val url0 = MessageFormat.format(url, URLEncoder.encode(teamName, "UTF-8"))
          self ! PrependUrl(teamName, url0, lastScrapDt getOrElse(DateTime.now - 10.years))
        }
      } catch {
        case ex => log.info("Mongo DB error" + ex.getMessage); self ! ScrapDone
      }
    }

    case PrependUrl(teamName, url, lastScrapDt) => {
      scheduledUrls = url :: scheduledUrls
      scheduledTeamNames = teamName :: scheduledTeamNames
      scrapers ! StartCollect(teamName, url, lastScrapDt)
    }

    case ProcessedResults(map, scrapDt) => {
      if (map.values.head != Nil )
        teamsResults = map.values.head ::: teamsResults

      scheduledUrls = scheduledUrls copyWithout(map.keys.head)

      if (scheduledUrls.isEmpty)
        self ! SaveResults(scrapDt)
    }

    case SaveResults(scrapDt: DateTime) => {
      import scala.collection.convert.WrapAsJava._
      log.info(s"Collect result size ${teamsResults.size}")
      //store result
      targetCollection insert(teamsResults, WriteConcern.JOURNALED)

      db getCollection(statTableName) insert(
        BasicDBObjectBuilder.start(
          Map("scrapDt" -> scrapDt.toDate, "affectedRecordsNum" -> teamsResults.size)).get)

      // recreate statistics
      val results = db getCollection("results")
      for ( (k, v) <- standingMeasurement) {
        db getCollection(k) drop
        val mapReduceCommand = new MapReduceCommand(results, v, reduce, k, MapReduceCommand.OutputType.REPLACE, null)
        results mapReduce mapReduceCommand
      }

      mongoClient close

      scheduledTeamNames foreach { teamName => recents ! UpdateRecent(teamName) }
    }

    case UpdateCompiled(team, status) => {
      log.info(s"UpdateCompiled  ${team} ${status}")
      scheduledTeamNames = scheduledTeamNames copyWithout(team)

      if (scheduledTeamNames.isEmpty)
        self ! ScrapDone
    }

    case ScrapDone => log.info(s"ScrapDone ScraperRootActor ${scheduledTeamNames.size} ${scheduledUrls.size}")
  }
}