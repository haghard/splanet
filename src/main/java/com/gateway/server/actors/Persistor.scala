package com.gateway.server.actors

import akka.actor.{ActorRef, ActorLogging, Actor}
import com.gateway.server.actors.Persistor.{UpdateCompiled, UpdateRecent}
import com.mongodb.{BasicDBObjectBuilder, BasicDBObject, MongoClient}
import java.util
import com.gateway.server.exts.MongoConfig
import org.vertx.java.core.logging.Logger

object Persistor {

  case class UpdateRecent(teamName: String)

  case class UpdateCompiled(teamName: String, status: String)

}

class Persistor(val mongoConfig: MongoConfig, val recentNum: Int) extends Actor with ActorLogging {

  var mongoClient: MongoClient = _

  def receive: Actor.Receive = {

    case UpdateRecent(teamName) => {
      try {
        log.info(s"Start recent for ${teamName} ")

        val ids = new util.ArrayList[String](recentNum)
        mongoClient = new MongoClient(mongoConfig.ip, mongoConfig.port)
        val db = mongoClient getDB (mongoConfig.db)

        if (!db.authenticate(mongoConfig.username, mongoConfig.password.toCharArray))
          throw new IllegalStateException("Update recent error. Authentication failed")

        val collection = db getCollection ("results")

        val cursor = collection.find(
          new BasicDBObject("$or", util.Arrays.asList(
            BasicDBObjectBuilder start("homeTeam", teamName) get,
            BasicDBObjectBuilder start("awayTeam", teamName) get
          )),
          BasicDBObjectBuilder start("_id", 1) get)
          .sort(BasicDBObjectBuilder start("dt", -1) get).limit(recentNum)

        while (cursor.hasNext) {
          ids.add(cursor.next.get("_id").asInstanceOf[String])
        }

        val recentCollection = db getCollection ("recent")

        recentCollection update(
          BasicDBObjectBuilder start("name", teamName) get,
          BasicDBObjectBuilder.start("$set",
            BasicDBObjectBuilder start("games_id", ids) get
          ).get)

        log.info(s"Recent for ${teamName} was updated")

        sender ! UpdateCompiled(teamName, "success")

      } catch {
        case ex => sender ! UpdateCompiled(teamName, ex.getMessage)
      } finally {
        mongoClient close
      }
    }
  }
}