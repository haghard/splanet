package com.gateway.server.test.unit

import scala.Predef._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import com.gateway.server.exts._
import org.scalatest.junit.JUnitRunner
import com.gateway.server.exts.MongoConfig
import com.mongodb._
import java.util
import java.util.Date
import org.joda.time.DateTime
import com.gateway.server.exts.MongoConfig
import java.text.SimpleDateFormat
import com.gateway.server.IO.{AsyncWebClient, WebClient}

//import com.mongodb.{BasicDBObjectBuilder, BasicDBObject, MongoClient}
import com.gateway.server.actors.{ScraperApplication, MongoDriverDao, Dao}
import com.escalatesoft.subcut.inject.NewBindingModule._
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}

@RunWith(classOf[JUnitRunner])
class ActorsTest extends FunSuite {

  implicit val module = newBindingModule { module =>
    import module._
    import scala.concurrent.duration._
    import com.escalatesoft.subcut.inject._

    bind [Dao] to newInstanceOf [MongoDriverDao]
    bind[String].idBy(MongoResponseArrayKey).toSingle("results")

    bind[MongoConfig].toSingle(MongoConfig("192.168.0.143", 27017, "nba", "", ""))
    bind[List[String]].toSingle(List("Oklahoma City Thunder"/*, "Miami Heat", "Chicago Bulls"*/))
    bind[String].idBy(ScraperUrl).toSingle("http://allbasketball.ru/teams/{0}.html")
    bind[String].idBy(ScraperStatCollectionKey).toSingle("scrapStat")
    bind[String].idBy(RecentCollectionKey).toSingle("recent")
    bind[String].idBy(SettingCollectionKey).toSingle("settings")

    bind[FiniteDuration].idBy(ScraperDelay).toSingle(0 second)
    bind[FiniteDuration].idBy(ScraperPeriod).toSingle(20 second)

  }

  class DaoMock(implicit val bindingModule: BindingModule) extends Injectable
  /*
   test("mongo agg framework") {

     val group = new BasicDBObject("$group",
         new BasicDBObject("_id", "$homeTeam").append(
         "wins", new BasicDBObject("$sum",
           new BasicDBObject("$cond",
             util.Arrays.asList(new BasicDBObject("$gt", util.Arrays.asList("$homeScore", "$awayScore")), 1, 0 )
         ))
         )
     )

     val mongoClient = new MongoClient("192.168.0.143", 27017)
     val db = mongoClient.getDB("sportPlanet")
     val coll = db.getCollection("results")

     println(s"res ${group}")
     val out = coll aggregate(group)
     println(out.getCommandResult.get("result"))
   }

   test("insert rec") {
     val now = DateTime.now
     val rec = new BasicDBObject("_id", "e743e757-d3cd-4d71-927e-8932b48fffff")
       .append("dt", now.toDate)
       .append("homeTeam", "TeamA")
       .append("awayTeam", "TeamB")
       .append("homeScore", 34)
       .append("awayScore", 13)
       .append("awayScore2", 11)

     val mongoClient = new MongoClient("192.168.0.143", 27017)
     val db = mongoClient.getDB("sportPlanet")
     val coll = db.getCollection("results")
     Try {
       //coll insert(rec, WriteConcern.SAFE)
       val rec0 = coll.find(new BasicDBObject("_id", "e743e757-d3cd-4d71-927e-8932b48fffff"))
       if (rec0.hasNext)
         println(new DateTime(rec0.next.get("dt").asInstanceOf[Date]))

       //throw new MongoException("Manually raised exception")
     } recover {
       case e: Throwable =>
         println(e.getMessage)
     }
   }

    test("dt ") {
      val lineDt = Array("2031", "11", "23T4:00:00").mkString("-")
      val currentDt = new DateTime(lineDt)
      println(currentDt)
    }
   */

   test(" test dt ") {
      new ScraperApplication().start
      Thread.sleep(120000);
   }

  /*test(" test dt ") {
    val dao = new DaoMock {
      val dao = inject[Dao]
      def action = { dao.open; dao.lastScrapDt }
      def close = dao.close
    }

    //new DateTime(
    println(dao action)

    dao.close
  }*/

  test("testMapReduce") {
    import scala.collection.JavaConversions._
    //import com.github.nscala_time.time.Imports._
    try {

      val mongo = new MongoClient(
        util.Arrays.asList(new ServerAddress("troup.mongohq.com", 10067)),
        util.Arrays.asList(MongoCredential.createMongoCRCredential("haghard", "sportPlanet", "suBai3sa".toCharArray))
      )

      //val creds = MongoCredential.createPlainCredential("haghard", "sportPlanet", "suBai3sa".toCharArray)
      val db = mongo.getDB("sportPlanet");

      //println(db.authenticate("haghard", "suBai3sa".toCharArray))

      val collection = db.getCollection("results")
      println(collection.getName)

      val t = DateTime.now.minusDays(1)

      val dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
      println(dateParser.format(t.toDate))

      val q = new BasicDBObject("dt", new BasicDBObject("$gt", t.toDate))

      println(q)
      val res = collection.find(q)

      while (res.hasNext) {
        val map = res.next.toMap

        val it = map.keySet.iterator
        while(it.hasNext)
          println(it.next.getClass)

        println(map.values.map({ x => x.getClass }))

      }


      /*val homeWinMap = "function () { if (this.homeScore > this.awayScore) emit( this.homeTeam, 1 ); }"
      val reduce = "function (key, values) { return Array.sum(values) }"
      val cmd = new MapReduceCommand(collection, homeWinMap, reduce, null, MapReduceCommand.OutputType.INLINE, null)
      val out = collection.mapReduce(cmd)
      val it = out.results.iterator

      while(it.hasNext)
        println(it.next.toString)*/

    } catch {
      case ex => println(ex)
    }
  }
}