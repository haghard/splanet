package com.gateway.server.actors

/*
import reactivemongo.api._
import scala.concurrent.ExecutionContext.Implicits.global
*/

import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.gateway.server.exts.{MongoResponseArrayKey, ScraperStatCollection, MongoConfig}
import java.util.Date
import scala.collection.JavaConversions._
import com.mongodb._
import scala.Some
import com.github.nscala_time.time.TypeImports.DateTime

trait Dao extends Injectable {

  val homeWinMap = "function () { if (this.homeScore > this.awayScore) emit(this.homeTeam, 1); }"
  val awayWinMap = "function () { if (this.homeScore < this.awayScore) emit(this.awayTeam, 1); }"
  val homeLoseMap = "function () { if (this.homeScore < this.awayScore) emit(this.homeTeam, 1); }"
  val awayLoseMap = "function () { if (this.homeScore > this.awayScore) emit(this.awayTeam, 1); }"
  val reduce = "function (key, values) { return Array.sum(values) }"

  val standingMeasurement = Map("homeWin" -> homeWinMap, "homeLose" -> homeLoseMap,
    "awayWin" -> awayWinMap, "awayLose" -> awayLoseMap)

  def mongoConfig = inject[MongoConfig]

  def scrapCollection = inject[String](ScraperStatCollection)

  def resultCollection = inject[String](MongoResponseArrayKey)

  def lastScrapDt: Option[DateTime]

  def storeResults(results: List[BasicDBObject])

  def saveScrapStat(dt: Date, size: Int)

  def updateStanding

  def open

  def close
}

class MongoDriverDao(implicit val bindingModule: BindingModule) extends Dao {
  var mongoClient: MongoClient = _
  var db: DB = _

  override def lastScrapDt: Option[DateTime] = {
    import scala.collection.JavaConversions._
    import com.github.nscala_time.time.Imports._
    val result = for {
      statCollectionName <- db getCollectionNames;
      if (statCollectionName == scrapCollection)
    } yield {
      val statColl = db getCollection statCollectionName
      val cursor = statColl.find().sort(
        BasicDBObjectBuilder.start("scrapDt", -1).get)
      if (cursor.hasNext) {
        val statObject = cursor.next
        val dt = new DateTime(statObject.get("scrapDt").asInstanceOf[Date])
        Some(dt)
      } else {
        None
      }
    }

    result.flatten.lastOption
  }

  override def storeResults(results: List[BasicDBObject]) = {
    db getCollection (resultCollection) insert(results, WriteConcern.SAFE)
  }

  override def saveScrapStat(dt: Date, size: Int) = {
    db getCollection (scrapCollection) insert (
      BasicDBObjectBuilder.start(
        Map("scrapDt" -> dt, "affectedRecordsNum" -> size)).get, WriteConcern.SAFE)
  }

  override def updateStanding = {
    val results = db getCollection (resultCollection)
    for ((k, v) <- standingMeasurement) {
      db getCollection (k) drop
      val mapReduceCommand = new MapReduceCommand(results, v, reduce, k, MapReduceCommand.OutputType.REPLACE, null)
      results mapReduce mapReduceCommand
    }
  }

  override def close = mongoClient.close

  override def open: Unit = {
    mongoClient = new MongoClient(mongoConfig.ip, mongoConfig.port)
    db = mongoClient getDB (mongoConfig.db)
  }
}