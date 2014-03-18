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
import com.gateway.server.exts.{RecentCollectionKey, MongoResponseArrayKey, ScraperStatCollectionKey, MongoConfig}
import scala.util.Try

trait Dao extends Injectable {

  val homeWinMap = "function () { if (this.homeScore > this.awayScore) emit(this.homeTeam, 1); }"
  val awayWinMap = "function () { if (this.homeScore < this.awayScore) emit(this.awayTeam, 1); }"
  val homeLoseMap = "function () { if (this.homeScore < this.awayScore) emit(this.homeTeam, 1); }"
  val awayLoseMap = "function () { if (this.homeScore > this.awayScore) emit(this.awayTeam, 1); }"
  val reduce = "function (key, values) { return Array.sum(values) }"

  val standingMeasurement = Map("homeWin" -> homeWinMap, "homeLose" -> homeLoseMap,
    "awayWin" -> awayWinMap, "awayLose" -> awayLoseMap)

  def mongoConfig = inject[MongoConfig]

  def scrapCollection = inject[String](ScraperStatCollectionKey)

  def resultCollection = inject[String](MongoResponseArrayKey)
  
  def recentCollection = inject[String](RecentCollectionKey)

  /**
   *
   */
  def open: Boolean

  /**
   *
   */
  def close

  /**
   *
   * @return
   */
  def lastScrapDt: Option[DateTime]

  /**
   *
   * @param results
   * @param dt
   * @param size
   * @return Try[WriteResult]
   *
   */
  def persist(results: List[BasicDBObject], dt: Date, size: Int): Try[WriteResult]

  /**
   *
   * @param teamName
   * @param recentNum
   */
  def updateRecent(teamName: String, recentNum: Int)

  /**
   *
   */
  def updateStanding

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

  /**
   * This is 2 step transaction
   * 1. Insert results
   * 2. Update scrap stat
   *
   * If 1 step will fail we just return Failed
   * If 2 step will fail we delete 1 step inserted objects
   *
   * @param results
   * @param dt
   * @param size
   * @return Try[WriteResult]
   *
   */
  override def persist(results: List[BasicDBObject], dt: Date, size: Int): Try[WriteResult] = {
    Try { db.getCollection(resultCollection).insert(results) }
      .flatMap { r => Try { db.getCollection(scrapCollection).insert(
         BasicDBObjectBuilder.start(Map("scrapDt" -> dt, "affectedRecordsNum" -> size)).get) }.recover {
        case th: Throwable => {
          val coll = db.getCollection(resultCollection)
          val cleanQuery = new BasicDBObject("_id", new BasicDBObject("$in", seqAsJavaList(results.map(_.get("_id").asInstanceOf[String]))))
          coll.remove(cleanQuery)
        }
      }
    }
  }

  override def updateStanding = {
    val results = db getCollection (resultCollection)
    for ((k, v) <- standingMeasurement) {
      db getCollection (k) drop
      val mapReduceCommand = new MapReduceCommand(results, v, reduce, k, MapReduceCommand.OutputType.REPLACE, null)
      results mapReduce mapReduceCommand
    }
  }

  override def open: Boolean = {
    mongoClient = new MongoClient(mongoConfig.ip, mongoConfig.port)
    db = mongoClient getDB (mongoConfig.db)
    db setWriteConcern WriteConcern.JOURNALED

    if (!db.authenticate(mongoConfig.username, mongoConfig.password.toCharArray())) {
      throw new MongoException("mongo authenticate error")
    }

    true
  }

  /**
   *
   * @param teamName
   * @param recentNum
   *
   */
  override def updateRecent(teamName: String, recentNum: Int) = {
    val ids = new java.util.ArrayList[String](recentNum)
    val recCollection = db getCollection (recentCollection)
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