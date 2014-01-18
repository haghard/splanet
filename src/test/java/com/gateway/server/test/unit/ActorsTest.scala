package com.gateway.server.test.unit

import org.junit.Test
import com.gateway.server.actors.ScraperApplication
import com.escalatesoft.subcut.inject.NewBindingModule._
import com.gateway.server.exts._
import com.gateway.server.exts.MongoConfig

class ActorsTest {

  implicit val module = newBindingModule { module =>
    import module._
    import scala.concurrent.duration._

    bind[MongoConfig].toSingle(MongoConfig("192.168.0.143", 27017, "sportPlanet"))
    bind[List[String]].toSingle(List("Philadelphia 76ers", "Indiana Pacers"))
    bind[String].idBy(ScraperUrl).toSingle("http://allbasketball.ru/teams/{0}.html")
    bind[String].idBy(ScraperStatCollection).toSingle("scrapStatTest")

    bind[FiniteDuration].idBy(ScraperDelay).toSingle(0 second)
    bind[FiniteDuration].idBy(ScraperPeriod).toSingle(10 second)
  }

  @Test
  def testScraper() {
    new ScraperApplication().start
    //assert there
    Thread.sleep(25000);
  }
}