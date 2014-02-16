package com.gateway.server.actors

import akka.actor.{ Props, ActorSystem}
import com.gateway.server.actors.Receptionist.StartScraper
import com.gateway.server.exts._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import scala.concurrent.duration.FiniteDuration


class ScraperApplication(implicit val bindingModule: BindingModule) extends Injectable {

  val delay = inject[FiniteDuration](ScraperDelay)
  val period = inject[FiniteDuration](ScraperPeriod)

  def start {
    val actorSystem = ActorSystem("splanet-scraper-system")
    val scraperActor = actorSystem actorOf(Props.apply(new Receptionist()), "Receptionist")

    import scala.concurrent.ExecutionContext.Implicits.global
    actorSystem.scheduler.schedule(delay, period)(scraperActor ! StartScraper)
  }
}