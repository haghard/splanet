package com.gateway.server.actors

import akka.actor._
import com.gateway.server.actors.Receptionist.Go
import com.gateway.server.exts._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import scala.concurrent.duration.FiniteDuration
import com.typesafe.config.{ConfigFactory, Config}

class ScraperApplication(implicit val bindingModule: BindingModule) extends Injectable {

  val delay = inject[FiniteDuration](ScraperDelay)
  val period = inject[FiniteDuration](ScraperPeriod)
  val loadConfig: Config = ConfigFactory.load()
  val actorSystem = ActorSystem("splanet-system", loadConfig.getConfig("akka"))

  def start {
    implicit val dispatcher = actorSystem.dispatchers.lookup("scraper-dispatcher")

    val controller = actorSystem.actorOf(Props(new Controller).withDispatcher("scraper-dispatcher"), "Controller")
    actorSystem.scheduler.schedule(delay, period)(controller ! Go)
  }
}