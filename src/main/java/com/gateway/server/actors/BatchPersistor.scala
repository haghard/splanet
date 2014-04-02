package com.gateway.server.actors

import akka.actor.{Props, ActorLogging, Actor}
import com.github.nscala_time.time.Imports._
import com.gateway.server.actors.Receptionist.{UpdateCompiled, SaveResults}
import com.mongodb.BasicDBObject
import scala.util.{Failure, Success}
import com.gateway.server.actors.WebGetter.PersistLater

object BatchPersistor {
  def apply(dao: Dao, recentWindow: Int, updateBatch: List[BasicDBObject], scrapDt: DateTime,
            teamNames: List[String]): Props =
     Props(new BatchPersistor(dao, recentWindow, updateBatch, scrapDt, teamNames))
       .withDispatcher("db-dispatcher")
}

/**
 *
 * @param recentNum
 * @param updateBatch
 * @param scrapDt
 */
class BatchPersistor(dao: Dao, recentNum: Int, updateBatch: List[BasicDBObject], scrapDt: DateTime,
                     teamNames: List[String]) extends Actor with ActorLogging {

  def receive = ({
    case SaveResults => {
      if (updateBatch.size > 0) {
        dao.persist(teamNames, updateBatch, scrapDt.toDate) match {
          case Success(_) => {
            log.info("Result with size:{} was saved", updateBatch.size)
            sender ! UpdateCompiled
          }
          case Failure(ex) => {
            log.info("Try persist later:", ex.getMessage)
            sender ! PersistLater(updateBatch, scrapDt)
          }
        }
      }
    }
  }: Actor.Receive).andThen(_ => context.stop(self))
}