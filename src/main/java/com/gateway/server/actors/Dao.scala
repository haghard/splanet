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
import com.google.common.base.Strings
import java.util

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
   * @param teamNames
   * @param results
   * @param dt
   * @return
   */
  def persist(teamNames: List[String], results: List[BasicDBObject], dt: Date): Try[Unit]

  /**
   *
   * @param teamName
   * @param recentNum
   */
  def updateRecent(teamName: String, recentNum: Int): WriteResult
}

class MongoDriverDao(implicit val bindingModule: BindingModule) extends Dao {
  var mongoClient: MongoClient = _
  var db: DB = _

  override def open: Boolean = {
    val builder = new MongoClientOptions.Builder()
    builder.connectionsPerHost(10)
    builder.socketTimeout(10000)

    if (Strings.isNullOrEmpty(mongoConfig.username) || Strings.isNullOrEmpty(mongoConfig.password)) {
      mongoClient = new MongoClient(
        util.Arrays.asList(new ServerAddress(mongoConfig.ip, mongoConfig.port)),
        builder.build())
    } else {
      mongoClient = new MongoClient(
        util.Arrays.asList(new ServerAddress(mongoConfig.ip, mongoConfig.port)),
        util.Arrays.asList(MongoCredential.createMongoCRCredential(
          mongoConfig.username, mongoConfig.db, mongoConfig.password.toCharArray)),

        builder.build()
      )
    }

    db = mongoClient.getDB(mongoConfig.db)
    db.setWriteConcern(WriteConcern.SAFE)


    true
  }

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
   * 3. Update statistics if first two was ok
   *
   * If 1 step will fail we just return Failed
   * If 2 step will fail we delete 1 step inserted objects
   *
   * @param results
   * @param dt
   * @return Try[Unit]
   *
   */
  override def persist(teamNames: List[String], results: List[BasicDBObject], dt: Date): Try[Unit] = {
    Try({
      db.getCollection(resultCollection).insert(results)
      db.getCollection(scrapCollection).insert(
        BasicDBObjectBuilder.start(Map("scrapDt" -> dt, "affectedRecordsNum" -> results.size)).get)
    }).recoverWith({
      case th: Exception => {
        val coll = db.getCollection(resultCollection)
        val cleanQuery = new BasicDBObject("_id", new BasicDBObject("$in", seqAsJavaList(results.map(_.get("_id").asInstanceOf[String]))))
        coll.remove(cleanQuery)
        scala.util.Failure(new Exception("Persist error. Transaction was rollback"))
      }
    }).map({ r => Try {
      updateStanding
      teamNames foreach { team => updateRecent(team, 5) }
    }})
  }

  private def updateStanding = {
    val results = db getCollection (resultCollection)
    for ((k, v) <- standingMeasurement) {
      db getCollection (k) drop
      val mapReduceCommand = new MapReduceCommand(results, v, reduce, k, MapReduceCommand.OutputType.REPLACE, null)
      results.mapReduce(mapReduceCommand)
    }
  }

  /**
   *
   * @param teamName
   * @param recentNum
   *
   */
  override def updateRecent(teamName: String, recentNum: Int): WriteResult = {
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

    recCollection.update(
      BasicDBObjectBuilder.start("name", teamName).get,
      BasicDBObjectBuilder.start("$set", BasicDBObjectBuilder.start("games_id", ids).get).get
    )
  }

  override def close = mongoClient.close
}