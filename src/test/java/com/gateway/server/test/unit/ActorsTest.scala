package com.gateway.server.test.unit

import org.junit.{Ignore, Test}
import com.gateway.server.actors.ScraperApplication
import com.escalatesoft.subcut.inject.NewBindingModule._
import com.gateway.server.exts._
import com.gateway.server.exts.MongoConfig
import com.mongodb.{BasicDBObjectBuilder, MapReduceCommand, MongoClient}
import scala.Predef._
import com.gateway.server.exts.MongoConfig

class ActorsTest {

  implicit val module = newBindingModule { module =>
    import module._
    import scala.concurrent.duration._

    bind[MongoConfig].toSingle(MongoConfig("192.168.0.143", 27017, "sportPlanet"))
    bind[List[String]].toSingle(List("Philadelphia 76ers", "Indiana Pacers"))
    bind[String].idBy(ScraperUrl).toSingle("http://allbasketball.ru/teams/{0}.html")
    bind[String].idBy(ScraperStatCollection).toSingle("scrapStat")

    bind[FiniteDuration].idBy(ScraperDelay).toSingle(0 second)
    bind[FiniteDuration].idBy(ScraperPeriod).toSingle(610 second)
  }

  //@Test
  @Ignore
  def testScraper() {
    new ScraperApplication().start
    //assert there
    Thread.sleep(60000);
  }

  //@Test
  @Ignore
  def testScalaFor() {
    val f = List(1,2,3)
    val s = List(11,12,13)

    for { (f0,s0) <- f.zip(s) } yield {
      println(f0 + " " +  s0)
    }

    val map = Map("A" -> (3,1), "B" -> (6,1))

    println(
      map.toSeq.sortWith { _._2._1 > _._2._1 }
    )
  }

  //@Test
  @Ignore
  def testMapReduce() {
    try {
      val mongo = new MongoClient("192.168.0.143", 27017)
      val db = mongo.getDB("sportPlanet")
      val collection = db.getCollection("results")

      val homeWinMap = "function () { if (this.homeScore > this.awayScore) emit( this.homeTeam, 1 ); }"
      val reduce = "function (key, values) { return Array.sum(values) }"
      val cmd = new MapReduceCommand(collection, homeWinMap, reduce, null, MapReduceCommand.OutputType.INLINE, null)
      val out = collection.mapReduce(cmd)
      val it = out.results.iterator

      while(it.hasNext)
        println(it.next.toString)

    } catch {
      case ex =>
    }
  }


  @Test
  def mr() {
    val homeWinMap = "function () { if (this.homeScore > this.awayScore) emit( this.homeTeam, 1 ); }"
    val awayWinMap = "function () { if (this.homeScore < this.awayScore) emit( this.awayTeam, 1 ); }"
    val homeLoseMap = "function () { if (this.homeScore < this.awayScore) emit( this.homeTeam, 1 ); }"
    val awayLoseMap = "function () { if (this.homeScore > this.awayScore) emit( this.awayTeam, 1 ); }"
    val reduce = "function (key, values) { return Array.sum(values) }"
    val standingMeasurement = Map("homeWin" -> homeWinMap, "homeLose" -> awayWinMap, "awayWin" -> homeLoseMap, "awayLose" -> awayLoseMap)

    val mongo = new MongoClient("192.168.0.143", 27017)
    val db = mongo.getDB("sportPlanet")
    val results = db.getCollection("results")

    for ((outCollection, mapFunction) <- standingMeasurement) {
      val mapReduceCommand =
        new MapReduceCommand(results, mapFunction, reduce, outCollection, MapReduceCommand.OutputType.REPLACE, null)
      results mapReduce mapReduceCommand
    }
  }
}