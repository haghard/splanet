package com.gateway.server.actors

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import com.gateway.server.actors.ScraperRootActor.{StopScraper, StartScraper}
import com.gateway.server.exts.{MongoConfig, runnableToFunc}

object SportPlanetScraper {

  def main(args: Array[String]) {
    val config = ConfigFactory.load
    val actorSystem = ActorSystem("splanet-scraper-system")

    val teams: List[String] = config.getStringList("teams").toList
    val url = config.getString("url")
    val statCollection = config.getString("statCollection")

    val scraperActor = actorSystem actorOf(Props.apply(
        ScraperRootActor(teams, url, statCollection, MongoConfig("192.168.0.143", 27017, "sportPlanet"))), "ScraperActor")

    scraperActor ! StartScraper
  }
}
