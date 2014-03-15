package com.gateway.server.actors

import akka.actor.{ActorLogging, Actor}
import com.gateway.server.actors.BatchPersistor.{UpdateRecentBatch, UpdateCompiled }
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import scala.util.{Failure, Try, Success}

object BatchPersistor {
  case class UpdateRecentBatch(teams: List[String])
  case class UpdateCompiled(teamName: String, status: String)
}

/**
 *
 * @param recentNum
 *
 */
class BatchPersistor(val recentNum: Int)(implicit val bindingModule: BindingModule) extends Actor with ActorLogging with Injectable {
  private val dao = inject[Dao]

  def receive = ({
    case UpdateRecentBatch(teams) => Try {
      dao.open
      log.info("Updating recent results for: {}", teams.toString)
      teams foreach { team => dao.updateRecent(team, recentNum) }
    } match {
      case Success(_) => {
        dao.close
        teams foreach { team => sender ! UpdateCompiled(team, "success") }
      }
      case Failure(ex) => {
        log.info(ex.getMessage)
        dao.close
        teams foreach { team =>  sender ! UpdateCompiled(team, ex.getMessage) }
      }
    }
  }: Actor.Receive).andThen(_ => context.stop(self))
}