package com.gateway.server.actors

import akka.actor.{Cancellable, ActorRef, Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import com.gateway.server.actors.ScraperRootActor.StartScraper
import com.gateway.server.exts._
import com.escalatesoft.subcut.inject.{Injectable, BindingModule}
import org.vertx.java.core.logging.Logger
import com.escalatesoft.subcut.inject.NewBindingModule._
import com.gateway.server.exts.MongoConfig

class ScraperApplication(implicit val bindingModule: BindingModule) extends Injectable {

  val mongoConfig = MongoConfig(inject[String](MongoHost), inject[Int](MongoPort), inject[String](MongoDBName))

  def start {
    val config = ConfigFactory load
    val actorSystem = ActorSystem("splanet-scraper-system")

    implicit val spModule = newBindingModule {
      module =>
        import module._
        bind[MongoConfig].toSingle(mongoConfig)
        bind[List[String]].toSingle(config.getStringList("teams").toList)
        bind[String].idBy(ScraperUrl).toSingle(config getString ("url"))
        bind[String].idBy(ScraperStatCollection).toSingle(config getString ("statCollection"))
    }

    val scraperActor = actorSystem actorOf(Props.apply(new ScraperRootActor(spModule)), "ScraperActor")

    import scala.concurrent.duration._
    schedule(config getInt ("scrapPeriodInHour") seconds, scraperActor)

    def schedule(period: FiniteDuration, actor: ActorRef): Cancellable = {
      import scala.concurrent.ExecutionContext.Implicits.global
      actorSystem.scheduler.schedule(period, period)(scraperActor ! StartScraper)
    }
  }
}