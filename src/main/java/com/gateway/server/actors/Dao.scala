package com.gateway.server.actors

/*
import reactivemongo.api._
import scala.concurrent.ExecutionContext.Implicits.global
*/

import java.util.Date
import com.mongodb._
import scala.Some
import scala.collection.JavaConversions._
import com.github.nscala_time.time.TypeImports.DateTime
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.gateway.server.exts.{MongoResponseArrayKey, ScraperStatCollection, MongoConfig}

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

  def updateRecent(teamName: String, recentNum: Int)

  def updateStanding

  def open

  def close
}

class MongoDriverDao(implicit val bindingModule: BindingModule) extends Dao {
  var mongoClient: MongoClient = _
  var db: DB = _

  override def lastScrapDt: Option[DateTime] = {
    import com.github.nscala_time.time.Imports._
      val statColl = db getCollection scrapCollection
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

  override def open = {
    mongoClient = new MongoClient(mongoConfig.ip, mongoConfig.port)
    db = mongoClient getDB (mongoConfig.db)
    if (! db.authenticate(mongoConfig.username, mongoConfig.password.toCharArray()))
      throw new IllegalAccessException("mongo authenticate error")
  }

  /**
   *
   * @param teamName
   * @param recentNum
   *
   */
  override def updateRecent(teamName: String, recentNum: Int) = {
    val ids = new java.util.ArrayList[String](recentNum)
    val recCollection = db getCollection ("recent")
    val resCollection = db getCollection (resultCollection)

    val cursor = resCollection.find(
      new BasicDBObject("$or", java.util.Arrays.asList(
        BasicDBObjectBuilder start("homeTeam", teamName) get,
        BasicDBObjectBuilder start("awayTeam", teamName) get
      )),
      BasicDBObjectBuilder start("_id", 1) get)
      .sort(BasicDBObjectBuilder start("dt", -1) get).limit(recentNum)

    while (cursor.hasNext) {
      ids.add(cursor.next.get("_id").asInstanceOf[String])
    }

    recCollection update(
      BasicDBObjectBuilder.start("name", teamName).get,
      BasicDBObjectBuilder.start("$set", BasicDBObjectBuilder.start("games_id", ids).get).get
    )
  }

  override def close = mongoClient.close
}