package com.gateway.server.actors

import akka.actor.{Props, ActorLogging, Actor}
import akka.routing.SmallestMailboxRouter
import java.text.MessageFormat
import scala.collection.immutable.Map
import com.mongodb._
import com.github.nscala_time.time.Imports._
import scala.Some
import java.util.Date

object ScraperRootActor {

  case object StartScraper

  case object StopScraper

  case class SaveResults(scrapDt: DateTime)

  case class PrependUrl(teamName: String, url: String, lastScrapDt: DateTime)

  case class RemoveResultList(url: String)

  def apply(teams: List[String], url: String, statTableName: String): ScraperRootActor = new ScraperRootActor(teams, url, statTableName)
}

/**
 * Collect only home teams to await duplicate
 *
 * @param teams
 * @param url
 */
final class ScraperRootActor(val teams: List[String], val url: String, val statTableName: String) extends Actor with ActorLogging with CollectionImplicits {
  import ScraperActor._
  import ScraperRootActor._

  var urls = List[String]()
  var teamsResults = List[BasicDBObject]()
  var targetCollection: DBCollection = _
  var db: com.mongodb.DB = _

  val scrapers = context.actorOf(Props.apply(new ScraperActor(self)).withRouter(SmallestMailboxRouter(10)), name = "ScraperActor")

  private def findLastScrapDt(db: DB, collectionName: String): Option[DateTime] = {
    import scala.collection.JavaConversions._
    val result = for {
      statCollectionName <- db.getCollectionNames
      if (statCollectionName == collectionName)
    } yield {
      val statColl = db getCollection(statCollectionName)
      val cursor = statColl.find().sort(BasicDBObjectBuilder.start("scrapDt", -1).get)
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
        val mongoClient = new MongoClient("192.168.0.143", 27017)
        db = mongoClient getDB("sportPlanet")
        targetCollection = db.getCollection("recent2")
        val lastScrapDt: Option[DateTime] = findLastScrapDt(db, statTableName)

        teams.foreach { team =>
          val url0 = MessageFormat.format(url, team)
          self ! PrependUrl(team.replaceAll("%20"," "), url0, lastScrapDt.getOrElse(DateTime.now - 10.years))
        }

      } catch {
        case ex => log.info("Mongo DB error" + ex.getMessage); self ! StopScraper
      }
    }

    case PrependUrl(teamName, url, lastScrapDt) => {
      urls = url :: urls
      scrapers ! TeamResultPage(teamName, url, lastScrapDt)
    }

    case ProcessedResults(map, scrapDt) => {
      if (map.values.head != Nil )
        teamsResults = map.values.head ::: teamsResults

      urls = urls.copyWithout(map.keys.head)

      if (urls.isEmpty)
        self ! SaveResults(scrapDt)
    }

    case SaveResults(scrapDt: DateTime) => {
      import scala.collection.convert.WrapAsJava._

      log.info(s"Start save results in db ${teamsResults.size}")

      targetCollection.insert(teamsResults, WriteConcern.JOURNALED)
      db.getCollection(statTableName).insert(
        BasicDBObjectBuilder.start(
          Map("scrapDt" -> scrapDt.toDate, "affectedRecordsNum" -> teamsResults.size)).get)

      log.info("Save all in db")
      self ! StopScraper
    }

    case StopScraper => {
      log.info("Stop system")
      context.system.shutdown()
    }
  }
}




