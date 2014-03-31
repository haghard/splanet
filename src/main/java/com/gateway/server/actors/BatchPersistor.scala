package com.gateway.server.actors

import akka.actor.{ActorLogging, Actor}
import com.github.nscala_time.time.Imports._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.gateway.server.actors.Receptionist.{UpdateCompiled, SaveResults}
import com.mongodb.BasicDBObject
import scala.util.{Failure, Success}
import com.gateway.server.actors.WebGetter.PersistLater

/**
 *
 * @param recentNum
 * @param updateBatch
 * @param scrapDt
 * @param bindingModule
 */
class BatchPersistor(dao: Dao, recentNum: Int, updateBatch: List[BasicDBObject], scrapDt: DateTime)
                    (implicit val bindingModule: BindingModule) extends Actor with ActorLogging with Injectable {
  private val teamNames = inject[List[String]]

  override def postRestart(reason: Throwable): Unit = {
    log.info(s" BatchPersistor was restarted ${reason.getMessage}")
  }

  def receive = ({
    case SaveResults => {
      if (updateBatch.size > 0) {
        log.info("Results size: {} ", updateBatch.size)
        dao.persist(teamNames, updateBatch, scrapDt.toDate) match {
          case Success(_) => sender ! UpdateCompiled
          case Failure(ex) => {
            log.info(ex.getMessage);
            sender ! PersistLater(updateBatch, scrapDt)
          }
        }
      }
    }
  }: Actor.Receive).andThen(_ => context.stop(self))
}