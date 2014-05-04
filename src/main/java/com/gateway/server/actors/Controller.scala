package com.gateway.server.actors

import akka.actor._
import java.net.URLEncoder
import java.text.MessageFormat
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import com.gateway.server.actors.Receptionist._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.gateway.server.actors.Receptionist.Go
import com.gateway.server.actors.Receptionist.Connected
import com.gateway.server.actors.Controller.BackOff
import akka.actor.Terminated
import akka.actor.Status.Failure
import com.gateway.server.exts.ScraperUrl

object Controller {

  case class BackOff(min: Int, max: Int) {
    val rnd = ThreadLocalRandom.current

    def waitTime(): (FiniteDuration, BackOff) = {
      val delay = rnd.nextInt(1, min)
      (new FiniteDuration(delay, TimeUnit.SECONDS), new BackOff(Math.min(max, 2 * min), max))
    }
  }
}

class Controller(implicit val bindingModule: BindingModule) extends Actor with Injectable with ActorLogging {
  import context.dispatcher

  var backOff = BackOff(10, 20)
  var receptionist: Option[ActorRef] = None

  override def receive = idle()

  def idle(): Receive = {
    case Go => context.become(switch())
  }

  def switch(): Receive = {
    receptionist = Some(context.actorOf(Props(new Receptionist).withDispatcher("scraper-dispatcher"), "Receptionist"))
    context.watch(receptionist.get)
    active()
  }

  def active(): Receive = {
    case Go => log.info("Ignore scheduling since we are active now")

    case Connected(dt, stages) => {
      val teamNames = inject[List[String]]
      val url = inject[String](ScraperUrl)
      teamNames foreach { x =>
        receptionist.get ! Go(TargetUrl(x, MessageFormat.format(url, URLEncoder.encode(x, "UTF-8")), dt), stages)
      }
    }

    case Failure(ex) => {
      receptionist.get ! Kill
      receptionist = None
    }

    case Terminated(actor) => {
      val next = backOff.waitTime
      backOff = backOff.waitTime._2
      log.info(s"Job was delayed. Wait ${next._1} before next run")
      context.system.scheduler.scheduleOnce(next._1, self, Go)
      context.become(idle())
    }

    case Done => {
      context.unwatch(receptionist.get)
      log.info("Scraping done")
      context.become(idle())
    }
  }
}