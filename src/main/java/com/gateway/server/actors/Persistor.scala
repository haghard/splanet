package com.gateway.server.actors

import akka.actor.{ActorLogging, Actor}
import com.gateway.server.actors.Persistor.{UpdateCompiled, UpdateRecent}
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import scala.util.{Failure, Try, Success}

object Persistor {

  case class UpdateRecent(teamName: String)

  case class UpdateCompiled(teamName: String, status: String)

}

/**
 *
 * @param recentNum
 *
 */
class Persistor(val recentNum: Int)(implicit val bindingModule: BindingModule) extends Actor with ActorLogging with Injectable {
  private lazy val dao = inject[Dao]

  def receive: Actor.Receive = {
    case UpdateRecent(teamName) =>
      Try {
        log.info(s"Start recent for ${teamName} ")
        // open/close connection on every message
        dao.open
        dao.updateRecent(teamName, recentNum)
      } match {
        case Success(_) => {
          dao.close; sender ! UpdateCompiled(teamName, "success")
        }
        case Failure(ex) => {
          log.debug(ex.getMessage)
          dao.close; sender ! UpdateCompiled(teamName, ex.getMessage)
        }
      }
  }
}