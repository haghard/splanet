package com.gateway.server.actors

import akka.actor._
import scala.concurrent._
import com.mongodb.BasicDBObject
import org.jsoup.Jsoup
import java.util.UUID
import com.github.nscala_time.time.Imports._
import scala.collection.immutable.Map
import java.util.concurrent.Executor

import akka.pattern.pipe
import akka.actor.Status.Failure
import scala.Some

object WebGetter {

  case class ComebackLater(teamName: String, url: String, lastScrapDt: DateTime)

  case class ProcessedResults(map: Map[String, List[BasicDBObject]], scrapDt: DateTime)

  case class Result(dt: String, homeTeam: String, awayTeam: String, homeScore: Int, awayScore: Int)

}

class WebGetter(teamName: String, url: String, lastScrapDt: DateTime) extends Actor with ActorLogging with ParserImplicits {
  import WebGetter._
  import scala.collection.immutable.Map

  private val timeExp = "\\[(\\d+):(\\d+)\\]".r
  private implicit val executor = context.dispatcher.asInstanceOf[Executor with ExecutionContext]

  override def postRestart(reason: Throwable) {
    super.postRestart(reason)
    log.info(s"${self.path} restarted because of ${reason.getMessage}")
  }

  extractResult(teamName, url)(lastScrapDt, DateTime.now) pipeTo self

  def receive: Receive = ({
    case result: ProcessedResults => context.parent ! result
    case Failure(ex) => {
      log.info(s"Try later for ${teamName}")
      context.parent ! ComebackLater(teamName, url, lastScrapDt)
    }
  }: Actor.Receive).andThen(_ => context.stop(self))

  private def extractResult(teamName: String, url: String)(startDt: DateTime, endDt: DateTime): Future[ProcessedResults] = future {
    import scala.collection.convert.WrapAsScala._
    val correctUrl = url.replace("+", "%20")
    log.info("Collect result from {} between {}  {}", url, lastScrapDt.toDate, endDt.toDate)

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
        val parts = dtText split (" ")
        if (parts.length != 2 || (parts(0).matches("(\\d\\d.){3}") && parts(1).matches("\\[.*\\]"))) {
          throw new IllegalArgumentException("Date parsing error")
        }

        val dt = parts(0) split ('.')
        val hours = parts(1) match {
          case timeExp(h, _) => h.toInt
          case _ => 0
        }

        val lineDt = Array("20" + dt(2), dt(1), s"${dt(0)}T${hours}:00:00").mkString("-")
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

    ProcessedResults(Map(url -> list.flatten), endDt)
  }
}
