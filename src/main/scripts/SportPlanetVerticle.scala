import io.vertx.rxcore.java.eventbus.RxEventBus
import org.vertx.scala.core.http.HttpServerRequest
import org.vertx.scala.platform.Verticle

class SportPlanetVerticle extends Verticle {

  lazy val html = <html>
    <head>
      <title>Title</title>
    </head>
    <body>
      <h1>Hello SportPlanetService!</h1>
    </body>
  </html>.toString()

  override def start() {
    vertx.createHttpServer().requestHandler { req: HttpServerRequest =>
      new RxEventBus(vertx.eventBus)
      req.response.end(html)
    }.listen(8080)
  }
}