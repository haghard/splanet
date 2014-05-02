package com.gateway.server.actors

import akka.actor._
import scala.concurrent._
import com.mongodb.BasicDBObject
import org.jsoup.Jsoup
import java.util.UUID
import com.github.nscala_time.time.Imports._
import scala.collection.immutable.Map

import akka.pattern.pipe
import akka.actor.Status.Failure
import scala.Some
import com.gateway.server.actors.Receptionist.TargetUrl

object WebGetter {

  case class ScrapLater(task: TargetUrl)

  case class PersistLater(updateBatch: List[BasicDBObject], scrapDt: DateTime)

  case class ProcessedResults(map: Map[TargetUrl, List[BasicDBObject]], scrapDt: DateTime)

  case class Result(dt: String, homeTeam: String, awayTeam: String, homeScore: Int, awayScore: Int)

  def apply(task: TargetUrl): Props = Props(new WebGetter(task))
    .withDispatcher("scraper-dispatcher")
}

class WebGetter(task: TargetUrl) extends Actor with ActorLogging with ParserImplicits {
  import WebGetter._
  import scala.collection.immutable.Map

  private val timeExp = "\\[(\\d+):(\\d+)\\]".r
  private implicit val executor = context.system.dispatchers.lookup("scraper-dispatcher")

  extractResult(task) (DateTime.now) pipeTo self

  def receive: Receive = ({
    case result: ProcessedResults => context.parent ! result
    case Failure(ex) => {
      log.info("Try scrap later for {}", task.teamName)
      context.parent ! ScrapLater(task)
    }
  }: Actor.Receive).andThen(_ => context.stop(self))

  private def extractResult(task: TargetUrl)(endDt: DateTime): Future[ProcessedResults] = future {
    import scala.collection.convert.WrapAsScala._
    val correctUrl = task.url.replace("+", "%20")
    log.info("Collect result from {} between {}  {}", task.url, task.startScrapDt.toDate, endDt.toDate)

    val doc = Jsoup.connect(correctUrl).ignoreHttpErrors(true).timeout(5000).get
    val table = doc.oneByClass("stat").toList

    val list = for {
      t <- table
      tr <- t getElementsByTag ("tr")
    } yield {
      val tds = tr getElementsByTag ("td")
      val scoreElements = tds(2) getElementsByTag ("span")

      if (tds(1).text == task.teamName &&
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

        val currentDt = new DateTime(lineDt).plusHours(4)
        if (currentDt >= task.startScrapDt && currentDt <= endDt) {
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

    ProcessedResults(Map(task -> list.flatten), endDt)
  }
}
