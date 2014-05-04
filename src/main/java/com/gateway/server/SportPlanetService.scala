package com.gateway.server

import QMongo._
import java.io.File
import org.vertx.java.core.logging.Logger
import org.vertx.java.core.buffer.Buffer
import com.google.common.hash.Hashing
import com.google.common.base.Charsets
import java.util.concurrent.atomic.AtomicInteger
import com.gateway.server.exts._
import java.net.{URLEncoder, URLDecoder}
import scala.collection.JavaConverters._
import scala.util.Try
import org.vertx.java.core.json.{JsonArray, JsonObject}
import io.vertx.rxcore.java.eventbus.{RxMessage, RxEventBus}
import com.escalatesoft.subcut.inject.{BindingModule, Injectable}
import org.vertx.java.core.http.{HttpServerRequest, RouteMatcher, HttpServer}
import scala.Predef._
import org.joda.time.DateTime
import rx.lang.scala.Observable
import rx.lang.scala.JavaConversions._
import scala.util.Failure
import scala.util.Success
import com.gateway.server.exts.Principal
import java.util.Date

object SportPlanetService {

  val USER_EMAIL = "email"

  val USER_PASSWORD = "password"

  val LOGIN_PAGE = "/examples/sportPlanet/login.html"

  val REPORTS_PAGE = "/examples/sportPlanet/center.html"

  val LOGIN_FAILED_PAGE = "/examples/sportPlanet/login_failed.html";

  val WEBROOT = new File(".").getAbsolutePath.replace(".", "") + "web/bootstrap"

  val wins = List("homeWin", "awayWin")
  val lose = List("homeLose", "awayLose")

  //api access errors
  //val error0 = "Invalid credential in request"
  val error1 = "Unauthorized access"


  val RECENT_STAT_URL = "/recent-stat/:team"
  val CONF_URL = "/conference"
  val STANDING_URL = "/standing/:flag"
  val STANDING2_URL = "/standing2"
  val RECENT_TEAM_URL = "/recent/:followedTeam"
  val RESULT_URL = "/results/:day"
  val TOURNAMENT_STAGE = "/stage/:day"

  def hash(password: String) =  Hashing.md5.newHasher.putString(password, Charsets.UTF_8).hash.toString
}

import SportPlanetService._


class SportPlanetService(implicit val bindingModule: BindingModule) extends Injectable {
  val logger = inject [Logger]
  val server = inject [HttpServer]

  val rxEventBus = inject [RxEventBus]
  val pModule = inject [String](MongoPersistorKey)

  val port = inject [Int](HttpServerPort)

  def eventBus(): RxEventBus = rxEventBus

  def start = {
    val router = new RouteMatcher

    /**
     * Rest resource /recent-stat/:team
     *
     **/
    import exts.{ fnToFunc1, fnToHandler1 }
    router get(RECENT_STAT_URL, { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        Try(URLDecoder.decode(req.params.get("team"), "UTF-8")).map({ teamName =>
          rxEventBus.send(pModule, recentStat(teamName, 10)).subscribe({ mes: RxMessage[JsonObject] =>
            RecentHealthProcessor(mes, teamName)
              .fold(req.response.end(new JsonObject().putString("status", "empty").toString))({ js: JsonObject =>
                  req.response.end(js.toString) })
          }, { ex: Throwable =>
            logger.error(ex.getMessage)
            req.response.end(new JsonObject().putString("status", ex.getMessage).toString)
          })
        }).recover {
          case ex: Exception => {
            logger.error(ex.getMessage);
            req.response.end(new JsonObject().putString("status", ex.getMessage).toString)
          }
        }
      }
    })

    /**
     * Basic http auth
     */
    /*router get("/auth", { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        readBasicAuth(req) fold(
        { failure => req.response.end(new JsonObject().putString("status","error").toString) },
        { p:(String, String) => rxEventBus.send[JsonObject, JsonObject](pModule, userByEmail(p._1))
            .subscribe({ mes: RxMessage[JsonObject] =>
              val followedTeams = ResponseFieldParser(mes, "followedTeams")
              val ft = URLEncoder.encode(followedTeams getOrElse(
                throw DBAccessException create(s"Response parse error [USER-followedTeams] : ${mes.body}")), "UTF-8")

              req.response.end(new JsonObject().putString("status","ok").putString("followed-teams", ft).toString)
          }, {
            th: Throwable => logger.info(th.getMessage)
              req.response.end(new JsonObject().putString("status","error").toString)
          })
        })
    }})*/

    router get(CONF_URL, { req: HttpServerRequest =>
      req.bodyHandler { buffer: Buffer =>
        rxEventBus.send[JsonObject, JsonObject](pModule, conferenceQuery).subscribe({ mes: RxMessage[JsonObject] =>
          Try(mes.body().getArray(MONGO_RESULT_FIELD)) match {
            case Success(array) => if (array.size() == 2) req.response().end(array.toString)
              else req.response.end(
                new JsonObject().putString("status", "error").putString("message","2 conference expected").toString)
            case Failure(ex) => req.response.end(
                new JsonObject().putString("status", "error").putString("message", ex.getMessage).toString)
          }
        }, {
          th: Throwable => logger.info(th.getMessage)
            req.response.end(
              new JsonObject().putString("status","error")
                .putString("ex", th.getMessage)
                .toString)
        })
      }
    })
    /**
     *  Rest resource /standing/:flag
     *  http://192.168.0.143:9000/standing/wins
     *  http://192.168.0.143:9000/standing/lose
     **/
    router get(STANDING_URL, { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        val flag = req.params.get("flag")

        def request(col: List[String]): rx.Observable[JsonObject] = {
          rx.Observable.merge(col.map { collectionName =>
            rxEventBus.send[JsonObject, JsonObject](pModule, standingQuery(collectionName))
          } .asJava)
            .reduce(new JsonObject().putArray("results", new JsonArray()), reduceAll)
        }

        val ob: Try[rx.Observable[JsonObject]] = flag match {
          case "wins" => Success(request(wins))
          case "lose" => Success(request(lose))
          case e => Failure(new IllegalArgumentException("unexpected param in request " + e))
        }

        ob match {
          case Success(ob) => ob.subscribe { message : JsonObject => req.response().end(message.toString) }
          case Failure(er) => { logger.info(er.getMessage);
            req.response.end(new JsonObject()
              .putString("status","error")
              .putString("message", er.getMessage)
              .toString)
          }
        }
      }
    })

    /**
     *
     */
    router get(STANDING2_URL, { req: HttpServerRequest =>
      req bodyHandler { buffer: Buffer =>
        def request(col: List[String]): rx.Observable[JsonObject] = {
          rx.Observable.merge(col.map { collectionName =>
            rxEventBus.send[JsonObject, JsonObject](pModule, standingQuery(collectionName))
          } .asJava).reduce(new JsonObject().putArray("results", new JsonArray()), reduceByMetrics)
        }
        request(wins ::: lose).subscribe({ json: JsonObject => req.response.end(json.toString) },
        { th: Throwable => logger.info(th.getMessage)
          req.response.end(new JsonObject().putString("status","error")
            .putString("body", th.getMessage).toString)
        })
      }
    })

    /**
     * Rest resource /recent/:followedTeam
     * Return recent games by single team
     *
     * JS simple client:
     *  function fetchRecent(team) {
     *   var promise = $.ajax({
     *       url: 'http://192.168.0.143:9000/recent/' + team
     *   }).promise();
     *   return Rx.Observable.fromPromise(promise);
     */
    router.get(RECENT_TEAM_URL, { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler { buffer: Buffer =>
        val teamName = URLDecoder decode(req.params.get("followedTeam"), "UTF-8")
        val responseWriter = new ChunkedResponseWriter(req, new AtomicInteger(1))

        val r = List(teamName).map({ teamName =>
          rxEventBus.send[JsonObject, JsonObject](pModule, recentResultByTeam(teamName))
        }) .map ({ ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.flatMap({ mes: RxMessage[JsonObject] =>
            val gamesId = ResponseFieldParserToArray(mes, "games_id")
            if (gamesId.isDefined) { rxEventBus.send[JsonObject, JsonObject](pModule, recentResultsById(gamesId.get)) }
            else { req.response.end; throw DBAccessException.create(s"Response parse error [RECENT GAMES ID] : ${mes.body}") }
          })
        }) .map { ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.subscribe( { mes: RxMessage[JsonObject] =>
            val responseChunk = ResponseParser(mes)
            if (responseChunk.isEmpty) {
              req.response.end; DBAccessException.create(s"Response parse error [RECENT GAMES] : ${mes.body}")
            } else {
              responseWriter.write(responseChunk.get)
            }
          }, { th: Throwable =>
            logger.info(th.getMessage)
            req.response.end(new JsonObject().putString("status","error")
              .putString("body", th.getMessage).toString)
          })
        }
      }
    })

    val parseDtError = Observable.items(new JsonObject().
      putString("status", "error").putString("message", "date parse error"))

    val stageError = Observable.items(new JsonObject().
      putString("status", "error").putString("message", "stage hould be playoff or regular"))

    def errorObs(msg: String) = Observable.items(new JsonObject()
      .putString("status", "error").putString("message", msg))

    /**
     *
     */
    import QMongo.dateFormatter
    import rx.lang.scala.Observable
    import rx.lang.scala.JavaConversions.toScalaObservable
    router.get(RESULT_URL, { req: HttpServerRequest =>
      req.bodyHandler { buffer: Buffer =>
        val finder: (Either[String, Principal]) => Observable[JsonObject] = { res =>
          res.fold({ mes => errorObs(mes)
          }, { p =>
            Try(dateFormatter.parse(req.params().get("day"))).map({ dt =>
              logger.info(s"${p} access ${RESULT_URL} ${dt}")
              currStage(dt) flatMap { stageName => stageName match {
                  case playoffEx(p, year) => {
                    val jObj = new JsonObject()
                    jObj.putString("stage", "playOff")
                    Observable.items(jObj)
                  }
                  case regularEx(p, year) =>
                    toScalaObservable(rxEventBus.send[JsonObject, JsonObject](pModule,
                      resultWindow(new DateTime(dt).minusDays(1).toDate, dt))).map({ m => m.body })

                  case _ => stageError
                }
              }
            }).getOrElse(parseDtError)
          })
        }

        reply(req, finder).subscribe { m:JsonObject => req.response.end(m.toString) }
      }
    })

    /**
     *
     *
     */
    router.get(TOURNAMENT_STAGE, { req: HttpServerRequest =>
      req.bodyHandler { buffer: Buffer =>
        val finder: (Either[String, Principal]) => Observable[JsonObject] = { res =>
          res.fold({ mes => errorObs(mes)
          }, { p =>
            logger.info(s"${p} access ${RESULT_URL} ")
            Try(dateFormatter.parse(req.params().get("day"))).map({ dt =>
              toScalaObservable(rxEventBus.send[JsonObject, JsonObject](pModule, currentStage(dt))).map({ m => m.body })
            }).getOrElse(parseDtError)
          })
        }

        reply(req, finder).subscribe { m: JsonObject => req.response().end(m.toString) }
      }
    })


    /**
     *  Return recent games by many teams
     *  passed as data parameter
     *
     *  JS simple client:
     *  function fetchRecent(teams) {
     *   var promise = $.ajax({
     *       url: 'http://192.168.0.143:9000/recent'
     *       data: 'followed-teams=' + teams
     *   }).promise();
     *   return Rx.Observable.fromPromise(promise);
     *
     *   teams - is a names separated by ,
     *   http://localhost:9000/recent?followed-teams=Chicago+Bulls%2CMiami+Heat
     *}
     */
    /*router get("/recent", { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler { buffer: Buffer =>
        if(Option(req.params.get("followed-teams")).isEmpty)
          throw IllegalHttpReqParams("Request param \"followed-teams\" expected ")

        val teams = req.params.get("followed-teams")
        val followedTeams = List(teams.split(","): _*).map({ line => line.substring(line.indexOf('=') + 1, line.length)})
        val responseWriter = new ChunkedResponseWriter(req, new AtomicInteger(followedTeams.size))

        val r = followedTeams.map({ teamName =>
          rxEventBus.send[JsonObject, JsonObject](pModule, recentResultByTeam(teamName))
        }) .map ({ ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.flatMap({ mes: RxMessage[JsonObject] =>
            val gamesId = ResponseFieldParserToArray(mes, "games_id")
            if (gamesId.isDefined) { rxEventBus.send[JsonObject, JsonObject](pModule, recentResultsById(gamesId.get)) }
            else { req.response.end; throw DBAccessException.create(s"Response parse error [RECENT GAMES ID] : ${mes.body}") }
          })
        }) .map { ob: rx.Observable[RxMessage[JsonObject]] =>
          ob.subscribe( { mes: RxMessage[JsonObject] =>
            val responseChunk = ResponseParser(mes)
            if (responseChunk.isEmpty) {
              req.response.end; DBAccessException.create(s"Response parse error [RECENT GAMES] : ${mes.body}")
            } else {
              responseWriter.write(responseChunk.get)
            }
          },
          { th: Throwable => logger.info(th.getMessage)
            req.response.end(new JsonObject().putString("status","error")
              .putString("body", th.getMessage).toString)
          })
        }
      }
    })*/

    router.getWithRegEx(".*", { req: HttpServerRequest =>
        req.path match {
          case "/" => req.response.sendFile(SportPlanetService.WEBROOT + SportPlanetService.LOGIN_PAGE)
          case r => req.response.sendFile(SportPlanetService.WEBROOT + URLDecoder.decode(req.path,"UTF-8"))
        }
    })

    import exts.fnToHandler1
    router.post("/center", { req: HttpServerRequest =>
      req.response.setChunked(true)
      req.expectMultiPart(true)
      req.bodyHandler({ buffer: Buffer =>
        if (!buffer.getString(0, buffer.length).matches("email=[\\w+@[a-zA-Z]+?\\.[a-zA-Z]{2,6}]+&password+=[\\w]+"))
          req.response.sendFile(WEBROOT + LOGIN_FAILED_PAGE)
        val email = req.formAttributes.get(USER_EMAIL)
        val passwordHash = hash(req.formAttributes.get(USER_PASSWORD))

        import exts.{ fnToAction1 }
        val t = rxEventBus.send[JsonObject, JsonObject](pModule, userQuery(passwordHash, email))
        .subscribe({ mes: RxMessage[JsonObject] =>
          val followedTeams = ResponseFieldParser(mes, "followedTeams")
          req.response.headers.add("Set-Cookie", "auth-user=" + email)
          req.response.headers.add("Set-Cookie", "followed-teams=" +
            URLEncoder.encode(followedTeams getOrElse(
              throw DBAccessException create(s"Response parse error [USER-followedTeams] : ${mes.body}")), "UTF-8"))

          req.response.sendFile(WEBROOT + REPORTS_PAGE)
        }, {
          th: Throwable => logger.info(th.getMessage)
            req.response.sendFile(WEBROOT + LOGIN_FAILED_PAGE)
        })
      })
    })

    server requestHandler(router)

    logger.info("Http server started")

    server listen(port)
  }

  /**
   * Provide anon access by default, should be mixed for secure purpose
   * @param req
   * @return
   */
  def lookupPrincipal(req: HttpServerRequest): rx.lang.scala.Observable[Either[String, Principal]] =
     Observable.items(Right(Principal("anon", "XXX")))

  private def reply(req: HttpServerRequest, finder: (Either[String, Principal]) => Observable[JsonObject]): Observable[JsonObject] =
    lookupPrincipal(req).map(finder).flatten

  private def currStage(dt: Date) = toScalaObservable(rxEventBus.send[JsonObject, JsonObject](pModule, currentStage(dt)))
    .map { m =>
    val results = m.body.getArray("results")
    results.get(0).asInstanceOf[JsonObject].getString("name")
  }

}