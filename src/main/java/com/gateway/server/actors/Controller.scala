package com.gateway.server.actors

import akka.actor._
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import com.gateway.server.actors.Receptionist._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import com.gateway.server.actors.Controller.BackOff
import akka.util.Timeout
import akka.actor.Status.Failure
import java.text.MessageFormat
import java.net.URLEncoder
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
  import scala.concurrent.duration._

  val teamNames = inject[List[String]]
  val url = inject[String](ScraperUrl)

  //defense from repeated scraper run
  private var busy = false
  var backOff = BackOff(10, 20)
  var receptionist: ActorRef = _

  override def receive: Actor.Receive = {
    case Go => if (!busy) {
      busy = true
      receptionist = context.actorOf(Props(new Receptionist).withDispatcher("scraper-dispatcher"), "Receptionist")
      context.watch(receptionist)
    } else {
      log.info("Ignore scheduling task while current in progress")
    }

    case Connected(dt) => {
      teamNames foreach { x =>
        receptionist ! Go(TargetUrl(x, MessageFormat.format(url, URLEncoder.encode(x, "UTF-8")), dt))
      }
    }

    case Failure(ex) => receptionist ! Kill

    case Terminated(actor) => {
      val next = backOff.waitTime
      backOff = backOff.waitTime._2
      log.info(s"Job was delayed. Wait ${next._1} before next run")
      busy = false
      context.system.scheduler.scheduleOnce(next._1, self, Go)
    }

    case Done => {
      busy = false
      context.unwatch(receptionist)
      log.info("Scrapping session done")
    }
  }
}