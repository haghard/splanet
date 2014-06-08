package com.gateway.server.actors

import akka.actor._
import scala.concurrent._
import com.mongodb.BasicDBObject
import org.jsoup.Jsoup
import java.util.UUID
import com.github.nscala_time.time.Imports._
import scala.collection.immutable.{TreeSet, Map}

import akka.pattern.pipe
import akka.actor.Status.Failure
import scala.Some
import com.gateway.server.actors.Receptionist.{Stage, TargetUrl}
import java.text.SimpleDateFormat
import scala.util.matching.Regex

object WebGetter {

  case class ScrapLater(task: TargetUrl)

  case class PersistLater(updateBatch: List[BasicDBObject], scrapDt: DateTime)

  case class GameResults(map: Map[TargetUrl, List[BasicDBObject]], scrapDt: DateTime)

  case class Result(dt: String, homeTeam: String, awayTeam: String, homeScore: Int, awayScore: Int)

  def apply(task: TargetUrl, stages: TreeSet[Stage]): Props = Props(new WebGetter(task, stages))
    .withDispatcher("scraper-dispatcher")
}

class WebGetter(task: TargetUrl, stages: TreeSet[Stage]) extends Actor with ActorLogging with ParserImplicits {
  import WebGetter._
  import scala.collection.immutable.Map

  private val timeExp = "\\[(\\d+):(\\d+)\\]".r
  private implicit val executor = context.system.dispatchers.lookup("scraper-dispatcher")

  //DateTime.now
  extractResult(task)(DateTime.now) pipeTo self
  //(new DateTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").parse("2014-04-19T00:00:00Z"))) pipeTo self


  def receive: Receive = ({
    case result: GameResults => context.parent ! result
    case Failure(ex) => {
      log.info("Try scrap later for {}", task.teamName)
      context.parent ! ScrapLater(task)
    }
  }: Actor.Receive).andThen(_ => context.stop(self))

  private def extractResult(task: TargetUrl)(scrapToDt: DateTime): Future[GameResults] = Future {
    import scala.collection.convert.WrapAsScala._
    val correctUrl = task.url.replace("+", "%20")
    log.info("Collect result from {} between {}  {}", task.url, task.startScrapDt.toDate, scrapToDt.toDate)

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
        val resultDt = new DateTime(lineDt).plusHours(4)

        if (resultDt >= task.startScrapDt && resultDt <= scrapToDt) {
          
          val resultStage = stages.find({ st => st.start.isBefore(resultDt) && st.end.isAfter(resultDt) })
          if(resultStage.isEmpty)
            throw new IllegalArgumentException("Unknown result stage")

          val mongoObject = new BasicDBObject("_id", UUID.randomUUID.toString)
            .append("dt", resultDt.toDate)
            .append("homeTeam", tds(1) text)
            .append("awayTeam", tds(3) text)
            .append("homeScore", scoreElements(0).text.toInt)
            .append("awayScore", scoreElements(1).text.toInt)
            .append("stage", resultStage.get.name)

          if (resultStage.get.name.contains("playoff")) {
            mongoObject.append("playoffKey", tds(1).text.concat(tds(3).text)
              .foldLeft(0) { (acc: Int, ch: Char) => acc + ch.toInt } )
          }

          Some(mongoObject)

        } else {
          None
        }
      } else {
        None
      }
    }

    GameResults(Map(task -> list.flatten), scrapToDt)
  }
}
