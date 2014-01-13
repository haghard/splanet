package com.gateway.server.actors

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._
import ScraperRootActor.StartScraper

object SportPlanetScraper {

  def main(args: Array[String]) {
    val config = ConfigFactory.load
    val actorSystem = ActorSystem("splanet-scraper-system")

    val teams: List[String] = config.getStringList("teams").toList
    val url = config.getString("url")
    val statCollection = config.getString("statCollection")

    val scraperActor = actorSystem actorOf(Props.apply(ScraperRootActor(teams, url, statCollection)), "ScraperActor")
    scraperActor ! StartScraper
  }

}
