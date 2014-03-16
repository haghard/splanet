package com.gateway.server.actors

import akka.actor.{ Props, ActorSystem}
import com.gateway.server.actors.Receptionist.StartScraper
import com.gateway.server.exts._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.{ConfigFactory, Config}


class ScraperApplication(implicit val bindingModule: BindingModule) extends Injectable {

  val delay = inject[FiniteDuration](ScraperDelay)
  val period = inject[FiniteDuration](ScraperPeriod)

  def loadConfig: Config = ConfigFactory.load()

  def start {
    val actorSystem = ActorSystem("splanet-system", loadConfig.getConfig("akka"))
    implicit val dispatcher = actorSystem.dispatchers.lookup("scraper-dispatcher")

    val receptionist = actorSystem.actorOf(Props(new Receptionist).withDispatcher("scraper-dispatcher"), "Receptionist")
    actorSystem.scheduler.schedule(delay, period)(receptionist ! StartScraper)
  }
}