package com.gateway.server.test.unit

import org.junit.{Ignore, Test}
import com.gateway.server.actors.ScraperApplication
import com.escalatesoft.subcut.inject.NewBindingModule._
import com.gateway.server.exts._
import com.mongodb.{MapReduceCommand, MongoClient}
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
    val s0 = map.toSeq.sortWith { _._2._1 > _._2._1 }*/
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

    val reduce0 = "function (key, values) { return Array.sum(values) }; "

    val reduce = "function (key, values) {var dict={}; " +
      "dict[\"Indiana Pacers\"]=\"e\";" +
      "dict[\"Miami Heat\"]=\"e\";" +
      "dict[\"Atlanta Hawks\"]=\"e\";" +
      "dict[\"Toronto Raptors\"]=\"e\";" +
      "dict[\"Chicago Bulls\"]=\"e\";" +
      "dict[\"Washington Wizards\"]=\"e\";" +
      "dict[\"Charlotte Bobcats\"]=\"e\";" +
      "dict[\"Brooklyn Nets\"]=\"e\";" +
      "dict[\"Detroit Pistons\"]=\"e\";" +
      "dict[\"New York Knicks\"]=\"e\";" +
      "dict[\"Cleveland Cavaliers\"]=\"e\";" +
      "dict[\"Boston Celtics\"]=\"e\";" +
      "dict[\"Philadelphia 76ers\"]=\"e\";" +
      "dict[\"Orlando Magic\"]=\"e\";" +
      "dict[\"Milwaukee Bucks\"]=\"e\";" +
      "dict[\"Oklahoma City Thunder\"]=\"w\";" +
      "dict[\"San Antonio Spurs\"]=\"w\";" +
      "dict[\"Portland Trail Blazers\"]=\"w\";" +
      "dict[\"Los Angeles Clippers\"]=\"w\";" +
      "dict[\"Houston Rockets\"]=\"w\";" +
      "dict[\"Golden State Warriors\"]=\"w\";" +
      "dict[\"Dallas Mavericks\"]=\"w\";" +
      "dict[\"Phoenix Suns\"]=\"w\";" +
      "dict[\"Minnesota Timberwolves\"]=\"w\";" +
      "dict[\"Denver Nuggets\"]=\"w\";" +
      "dict[\"Memphis Grizzlies\"]=\"w\";" +
      "dict[\"Los Angeles Lakers\"]=\"w\";" +
      "dict[\"New Orleans Pelicans\"]=\"w\";" +
      "dict[\"Sacramento Kings\"]=\"w\";" +
      "dict[\"Utah Jazz\"]=\"w\";" +
      "return { c: Array.sum(values), conf: dict[key] } ; }"

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