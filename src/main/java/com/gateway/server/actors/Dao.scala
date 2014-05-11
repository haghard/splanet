package com.gateway.server.actors

import java.util.Date
import com.mongodb._
import scala.collection.JavaConversions._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.gateway.server.exts._
import scala.util.Try
import com.google.common.base.Strings
import java.util
import scala.Some
import com.gateway.server.exts.MongoConfig
import org.joda.time.DateTime
import com.gateway.server.actors.Receptionist.Stage
import scala.collection.immutable.TreeSet
import com.gateway.server.actors.Dao.{AwayFormSettings, HomeFormSettings, FormSettings}

/*
  Map-Reduce prev version

  val homeWinMap = "function () { if (this.homeScore > this.awayScore) emit(this.homeTeam, 1); }"
  val awayWinMap = "function () { if (this.homeScore < this.awayScore) emit(this.awayTeam, 1); }"
  val homeLoseMap = "function () { if (this.homeScore < this.awayScore) emit(this.homeTeam, 1); }"
  val awayLoseMap = "function () { if (this.homeScore > this.awayScore) emit(this.awayTeam, 1); }"
  val reduce = "function (key, values) { return Array.sum(values) }"

  val standingMeasurement = Map("homeWin" -> homeWinMap, "homeLose" -> homeLoseMap,
    "awayWin" -> awayWinMap, "awayLose" -> awayLoseMap)

  for ((k, v) <- standingMeasurement) {
    db.getCollection(k).drop
    val mapReduceCommand = new MapReduceCommand(results, v, reduce, k, MapReduceCommand.OutputType.REPLACE, null)
    results.mapReduce(mapReduceCommand)
  }

*/

object Dao {

  trait FormSettings {
    
    val fields: Array[String] = Array("$homeScore", "$awayScore")

    def fieldName: String

    def wFieldConditions: java.util.List[String]

    def lFieldConditions: java.util.List[String]
  }

  case class HomeFormSettings(fieldName: String = "$homeTeam") extends FormSettings {
    override def wFieldConditions: java.util.List[String] = java.util.Arrays.asList(fields(0), fields(1))
    override def lFieldConditions: java.util.List[String] = java.util.Arrays.asList(fields(1), fields(0))
  }

  case class AwayFormSettings(fieldName: String = "$awayTeam") extends FormSettings {
    override def wFieldConditions(): java.util.List[String] = java.util.Arrays.asList(fields(1), fields(0))
    override def lFieldConditions(): java.util.List[String] = java.util.Arrays.asList(fields(0), fields(1))
  }
}

trait Dao extends Injectable {

  def mongoConfig = inject[MongoConfig]

  def scrapCollection = inject[String](ScraperStatCollectionKey)

  def resultCollection = inject[String](MongoResponseArrayKey)

  def settingCollection = inject[String](SettingCollectionKey)

  def open: Boolean

  def close

  def lastScrapDt: Option[DateTime]

  def currentStage: Option[Stage]

  def stages: TreeSet[Stage]

  def persist(teamNames: List[String], results: List[BasicDBObject], dt: Date): Try[Unit]

  def updateRecent(teamNames: List[String], stageName: String, recentNum: Int): Unit
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
    Try {
      db.getCollection(resultCollection).insert(results)
      db.getCollection(scrapCollection).insert(
        BasicDBObjectBuilder.start(Map("scrapDt" -> dt, "affectedRecordsNum" -> results.size)).get)
    } recoverWith {
      case th: Exception => {
        val coll = db.getCollection(resultCollection)
        val cleanQuery = new BasicDBObject("_id", new BasicDBObject("$in", seqAsJavaList(results.map(_.get("_id").asInstanceOf[String]))))
        coll.remove(cleanQuery)
        scala.util.Failure(new Exception("Persist error. Transaction was rollback", th))
      }
    } map {
      r => Try {
        val stageNames = results groupBy {
          dbObject => dbObject.get("stage").asInstanceOf[String]
        }
        updateStanding(stageNames.keySet, teamNames)
      }
    }
  }


  /**
  db.results.aggregate([{
      $match: { stage: "regular-2013/2014" }},
      { $group: { _id: { team:"$awayTeam" },
        w: { $sum: {  $cond : [ { $gt: [ "$awayScore", "$homeScore" ] }, 1, 0 ] } } ,
        l: { $sum: {  $cond : [ { $gt: [ "$homeScore", "$awayScore" ] }, 1, 0 ] } }
        }
      },

    { $project: { _id: 0,  team:"$_id.team", w:1, l:1 } }, { $sort: { team: 1 } }
    ])
    */
  private def updateStanding(stageNames: Set[String], teamNames: List[String]) = {
    stageNames foreach { stage =>
      stage match {
        case regularEx(k, y) => {
          updateForm(stage, HomeFormSettings())
          updateForm(stage, AwayFormSettings())
          updateRecent(teamNames, stage, 5)
        }
        case playoffEx(k, y) => {
          //to bring playoff pair-key
          updateForm(stage, HomeFormSettings())
          updateForm(stage, AwayFormSettings())
        }
      }
    }
  }

  private def updateForm(stageName: String, formSettings: FormSettings) = {
    val matchSection = new BasicDBObject("$match", new BasicDBObject("stage", stageName))

    val matchSection0 = new BasicDBObject("_id", new BasicDBObject("team", formSettings.fieldName))

    matchSection0.put("w", new BasicDBObject("$sum", new BasicDBObject("$cond",
      util.Arrays.asList(new BasicDBObject("$gt", formSettings.wFieldConditions), 1, 0))))

    matchSection0.put("l", new BasicDBObject("$sum", new BasicDBObject("$cond",
      util.Arrays.asList(new BasicDBObject("$gt", formSettings.lFieldConditions), 1, 0))))

    val groupSection = new BasicDBObject("$group", matchSection0)

    val projectSection0 = new BasicDBObject("_id", 0)
    projectSection0.put("team","$_id.team")
    projectSection0.put("w", 1)
    projectSection0.put("l", 1)

    val projectSection = new BasicDBObject("$project", projectSection0)

    val sortSection = new BasicDBObject("$sort", new BasicDBObject("team", 1))

    val cursor: Cursor = db.getCollection(resultCollection).aggregate(
      util.Arrays.asList(matchSection, groupSection, projectSection, sortSection),
      AggregationOptions.builder().build()
    )

    val collName = s"${formSettings.fieldName.drop(1)}-${stageName}"
    db.getCollection(collName).drop()
    val collection = db.getCollection(collName)

    while(cursor.hasNext)
      collection.insert(cursor.next)
  }

  override def updateRecent(teamNames: List[String], stageName: String, recentNum: Int): Unit = {
    val collName = s"recent-${stageName}"
    val recentColl = db.getCollection(collName)
    recentColl.drop

    teamNames foreach { team =>
      val ids = new java.util.ArrayList[String](recentNum)
      val resCollection = db getCollection (resultCollection)

      val cursor = resCollection.find(
        new BasicDBObject("$or", java.util.Arrays.asList(
          BasicDBObjectBuilder start("homeTeam", team) get,
          BasicDBObjectBuilder start("awayTeam", team) get
        )),
        BasicDBObjectBuilder start("_id", 1) get)
        .sort(BasicDBObjectBuilder start("dt", -1) get).limit(recentNum)

      while (cursor.hasNext) {
        ids.add(cursor.next.get("_id").asInstanceOf[String])
      }

      recentColl.insert(new BasicDBObject(team, BasicDBObjectBuilder.start("games_id", ids).get))
    }
  }

  override def currentStage(): Option[Stage] = {
    val now = DateTime.now()
    val setCollection = db.getCollection(settingCollection)

    Option(setCollection.findOne(
      new BasicDBObject("$and", java.util.Arrays.asList(
        new BasicDBObject("startDt", new BasicDBObject("$lt", now.toDate)),
        new BasicDBObject("endDt", new BasicDBObject("$gt", now.toDate))
      )
    ))).map({ obj => Stage(new DateTime(obj.get("startDt").asInstanceOf[Date]),
          new DateTime(obj.get("endDt").asInstanceOf[Date]),
          obj.get("name").asInstanceOf[String]) })
  }

  override def stages() = {
    val setCollection = db.getCollection(settingCollection)
    val cursor = setCollection.find()
    var stages = new TreeSet[Stage]()

    while(cursor.hasNext) {
      val obj = cursor.next()
      stages = stages + Stage(new DateTime(obj.get("startDt").asInstanceOf[Date]),
        new DateTime(obj.get("endDt").asInstanceOf[Date]),
        obj.get("name").asInstanceOf[String])
    }

    stages
  }

  override def close = mongoClient.close
}