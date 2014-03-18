package com.gateway.server.actors

import akka.actor._
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import com.gateway.server.actors.Receptionist.{Connected, TryLater, Done, Go}
import com.escalatesoft.subcut.inject.BindingModule
import com.gateway.server.actors.Controller.BackOff
import akka.util.Timeout
import akka.actor.Status.Failure

object Controller {

  case class BackOff(min: Int, max: Int) {
    val rnd = ThreadLocalRandom.current

    def waitTime(): (FiniteDuration, BackOff) = {
      val delay = rnd.nextInt(1, min)
      (new FiniteDuration(delay, TimeUnit.SECONDS), new BackOff(Math.min(max, 2 * min), max))
    }
  }
}

class Controller(implicit val bindingModule: BindingModule) extends Actor with ActorLogging {
  import context.dispatcher
  import scala.concurrent.duration._

  //defense from repeated scraper run
  private var busy = false
  implicit val timeout: Timeout = 5 seconds
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

    case Connected => receptionist ! Go

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