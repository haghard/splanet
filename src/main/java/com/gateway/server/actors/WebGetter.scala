package com.gateway.server.actors

import akka.actor.{ActorLogging, Actor, ActorRef}
import scala.concurrent._
import com.mongodb.BasicDBObject
import org.jsoup.Jsoup
import java.util.UUID
import com.github.nscala_time.time.Imports._
import scala.util.Failure
import scala.Some
import scala.util.Success
import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext.Implicits.global

object WebGetter {

  case class StartCollect(teamName: String, url: String, lastScrapDt: DateTime)

  case class ProcessedResults(map: Map[String, List[BasicDBObject]], scrapDt: DateTime)

  case class Result(dt: String, homeTeam: String, awayTeam: String, homeScore: Int, awayScore: Int)

}

import org.vertx.java.core.logging.Logger
class WebGetter(val scraperRootActor: ActorRef, val log: Logger) extends Actor with ParserImplicits {

  import WebGetter._
  import scala.collection.immutable.Map
  import com.github.nscala_time.time.Imports._

  def receive = {

    case StartCollect(teamName, url, lastScrapDt) => {
      val scrapDt = DateTime.now

      log.info(s"Collect result from ${url} between [${lastScrapDt.toDate.toString}]  [${scrapDt.toDate.toString}]")

       val future: Future[Map[String, List[BasicDBObject]]] = extractResult(teamName, url)(lastScrapDt, scrapDt)

      future onComplete {
        case Success(resultMap) => scraperRootActor ! ProcessedResults(resultMap, scrapDt)
        case Failure(er) => log.error(s"Can't process pageUrl, cause: ${er.getMessage}")
      }
    }
  }

  private def extractResult(teamName: String, url: String)(startDt: DateTime, endDt: DateTime): Future[Map[String, List[BasicDBObject]]] = future {
    import scala.collection.convert.WrapAsScala._
    val correctUrl = url.replace("+", "%20")
    try {
      val doc = Jsoup.connect(correctUrl).timeout(10000).get
      val table = doc.oneByClass("stat").toList

      val list = for {
        t <- table
        tr <- t getElementsByTag ("tr")
      } yield {
        val tds = tr getElementsByTag ("td")
        val scoreElements = tds(2) getElementsByTag ("span")

        if (tds(1).text == teamName &&
          (scoreElements(0).text.toInt != 0) && (scoreElements(1).text.toInt != 0)) {
          val dtText = tds(0).text.trim
          val parts = dtText split(" ")
          if (parts.length != 2 || (parts(0).matches("(\\d\\d.){3}") && parts(1).matches("\\[.*\\]"))) {
            throw new IllegalArgumentException("Date parsing error")
          }

          val dt = parts(0)
          val dt0 = dt split ('.')
          val lineDt = Array("20" + dt0(2), dt0(1), dt0(0)).mkString("-")
          val currentDt = new DateTime(lineDt)

          if (currentDt >= startDt && currentDt <= endDt) {
            Some(new BasicDBObject("_id", UUID.randomUUID.toString)
              .append("dt", currentDt.toDate)
              .append("homeTeam", tds(1) text)
              .append("awayTeam", tds(3) text)
              .append("homeScore", scoreElements(0).text.toInt)
              .append("awayScore", scoreElements(1).text.toInt))
          } else {
            None
          }
        } else {
          None
        }
      }

      log.info(s"Game played:  ${list.flatten.size} for ${teamName} ")
      Map(url -> list.flatten)

    } catch {
      case ex => {
        log.info(correctUrl + ":" + ex.getMessage); Map(url -> Nil)
      }
    }
  }
}
