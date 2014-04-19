package com.gateway.server;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import io.vertx.rxcore.java.eventbus.RxEventBus;
import io.vertx.rxcore.java.eventbus.RxMessage;
import io.vertx.rxcore.java.http.RxHttpServer;
import io.vertx.rxcore.java.http.RxHttpServerRequest;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;
import rx.Observable;
import rx.util.functions.Action1;
import rx.util.functions.Func1;
import rx.util.functions.Func2;
import rx.util.functions.FuncN;

import java.util.*;
import java.util.regex.Pattern;

import static com.gateway.server.MongoQueries.*;

/*
public class SportPlanetVerticle extends Verticle {
  private int port;
  private Logger logger;

  private JsonObject config;

  private static final Pattern pattern = Pattern.compile("[\\d]+");

  private static final String MONGO_RESULT_FIELD = "results";

  private static final String EVENT_BUS_NAME = "splanet/eventbus";
  private static final String MONGO_MODULE_NAME = "mongo-persistor";
  private static final String JDBS_MODULE_NAME = "com.bloidonia.jdbcpersistor";
  private static final String VERTIX_JDBC_MODULE_NAME = "com.bloidonia~mod-jdbc-persistor~2.1";

  private static final String VERTIX_MONGO_MODULE_NAME = "io.vertx~mod-mongo-persistor~2.1.1";

  private static final String LOGIN_PAGE = "/examples/sportPlanet/login.html";
  private static final String LOGIN_FAILED_PAGE = "/examples/sportPlanet/login_failed.html";
  private static final String REPORTS_PAGE = "/examples/sportPlanet/center.html";

  private final String webroot = new java.io.File(".").getAbsolutePath().replace(".", "") + "web/bootstrap";

  private void init() {
    logger = container.logger();
    config = container.config();
    logger.info("Config :" + config.toString());

    port = config.getObject("network-settings").getInteger("port");

    initPersistor(config);
    initEventBus();
  }

  public void start() {
    init();

    final RxHttpServer server = new RxHttpServer(vertx.createHttpServer());

    final RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());

    server.http().subscribe(new Action1<RxHttpServerRequest>() {
      @Override
      public void call(final RxHttpServerRequest req) {
        switch (req.method()) {
          case "GET": {
            logger.info("Http get: " + req.path());
            if (req.path().equals("/")) {
              req.response().sendFile(webroot + LOGIN_PAGE);
            } else {
              if (req.path().contains("signin.css") || req.path().contains("client.js") ||
                  req.path().contains("app.js")) {
                req.response().sendFile(webroot + "/examples/sportPlanet" + req.path());
              } else {
                req.response().sendFile(webroot + req.path());
              }
            }
          }

          case "POST": {
            if (req.path().contains("/recent")) {
              req.expectMultiPart(true);
              req.asObservable().flatMap(new Func1<Buffer, Observable<Map<String, JsonArray>>>() {
                @Override
                public Observable<Map<String, JsonArray>> call(Buffer buffer) {
                  final String ctx = new String(new String(buffer.getBytes()).replace('+', ' '));

                  final Iterable cleanTeams = Iterables.transform(Arrays.asList(ctx.split("&")), new Function<String, Object>() {
                    @Override
                    public Object apply(String item) {
                      return item.substring(item.indexOf('=') + 1, item.length());
                    }
                  });

                  final Iterator<String> teamNames = cleanTeams.iterator();

                  List<Observable<RxMessage<JsonObject>>> recents = new ArrayList<>();

                  while (teamNames.hasNext())
                    recents.add(searchSingle(createRecentIdsQuery(teamNames), rxEventBus));

                  return Observable.zip(recents, new FuncN<Map<String, JsonArray>>() {
                    @Override
                    public Map<String, JsonArray> call(Object... objects) {
                      final Map<String, JsonArray> result = new HashMap<>();
                      final RxMessage<JsonObject>[] array = new RxMessage[objects.length];
                      System.arraycopy(objects, 0, array, 0, objects.length);

                      for (RxMessage<JsonObject> message : array) {
                        final JsonArray jsonResults = message.body().getArray(MONGO_RESULT_FIELD);
                        //should be 1,
                        if (jsonResults.size() == 1) {
                          JsonObject json = jsonResults.get(0);
                          result.put(json.getString("teamName"), json.getArray("games_id"));
                        }
                      }
                      return result;
                    }
                  });
                }
              }).flatMap( new Func1<Map<String, JsonArray>, Observable<JsonArray>>() {
                @Override
                public Observable<JsonArray> call(Map<String, JsonArray> recentMap) {
                  final Observable<JsonArray> xs0 = Observable.from(recentMap.values());
                  final Observable<Observable<JsonArray>> yss =
                   xs0.map(new Func1<JsonArray, Observable<JsonArray>>() {
                     @Override
                     public Observable<JsonArray> call(final JsonArray array) {
                       return searchSingle(createRecentResultsQuery(array.iterator()), rxEventBus)
                        .map(new Func1<RxMessage<JsonObject>, JsonArray>() {
                          @Override
                          public JsonArray call(RxMessage<JsonObject> jsonObjectRxMessage) {
                            return jsonObjectRxMessage.body().getArray(MONGO_RESULT_FIELD);
                          }
                        });
                     }
                   });
                  //merge Observable of Observable stream
                  return Observable.merge(yss);
                }
              }).aggregate("", new Func2<String, JsonArray, String>() {
                @Override
                public String call(String acc, JsonArray jsonArray) {
                  final StringBuilder multipleResults = new StringBuilder();
                  for (Object result : jsonArray) {
                    multipleResults.append(result.toString()).append(',');
                  }

                  return new String(acc + multipleResults.toString());
                }
              }).subscribe(new Action1<String>() {
                  @Override
                  public void call(String recentResults) {
                    String json = null;
                    if (!Strings.isNullOrEmpty(recentResults)) {
                      json = "{ \"results\": [" + recentResults.substring(0, recentResults.length() - 1) + "] }";
                    } else {
                      json = "{ results: [] }";
                    }
                    req.response().end(json);
                  }
                },
                new Action1<Throwable>() {
                  public void call(Throwable err) {
                    logger.info(err.getMessage());
                  }
                }
              );
            } else if (req.path().contains("/center")) {
              final StringBuilder email = new StringBuilder();
              req.response().setChunked(true);
              req.expectMultiPart(true);
              req.asObservable().flatMap(new Func1<Buffer, Observable<RxMessage<JsonObject>>>() {
                @Override
                public Observable<RxMessage<JsonObject>> call(Buffer buffer) {
                  final String content = new String(buffer.getBytes());
                  final String[] userCreds = content.split("&");

                  email.append(userCreds[0].replace("email=", "").trim());
                  String password = userCreds[1].replace("password=", "").trim();

                  final String passwordHash = hash(password);
                  final JsonObject userQuery = createUserQuery(passwordHash, email.toString());
                  return rxEventBus.send(MONGO_MODULE_NAME, userQuery);
                }
              }).flatMap(new Func1<RxMessage<JsonObject>, Observable<RxMessage<JsonObject>>>() {
                @Override
                public Observable<RxMessage<JsonObject>> call(RxMessage<JsonObject> jsonObjectRxMessage) {
                  Observable<RxMessage<JsonObject>> resultObs = Observable.empty();
                  if (jsonObjectRxMessage.body().getArray(MONGO_RESULT_FIELD).size() == 1) {
                    final JsonObject result = jsonObjectRxMessage.body().getArray(MONGO_RESULT_FIELD).get(0);
                    //potential NPE
                    final Iterator ids = result.getArray("followedTeams").iterator();

                    logger.info("IDS:" + ImmutableList.copyOf(result.getArray("followedTeams")));
                    resultObs = searchSingle(createTeamQuery(ids), rxEventBus);

                    */
/*if (ids.hasNext()) {
                      resultObs = loop(searchSingle(createTeamQuery(ids), rxEventBus), ids, rxEventBus);
                    }*//*

                  }
                  return resultObs;
                }
              }).subscribe(new Action1<RxMessage<JsonObject>>() {
                 @Override
                 public void call(RxMessage<JsonObject> teams) {
                   final StringBuffer strings = new StringBuffer();
                   final JsonArray array = teams.body().getArray(MONGO_RESULT_FIELD);

                   boolean f = true;
                   for (Object item : array) {
                     if (f) {
                      strings.append(((JsonObject) item).getString("name"));
                      f = false;
                     } else {
                       strings.append(',').append(((JsonObject) item).getString("name"));
                     }
                   }

                   if (Strings.isNullOrEmpty(strings.toString())) {
                     req.response().sendFile(webroot + LOGIN_FAILED_PAGE);
                   } else {
                     req.response().headers().add("Set-Cookie", "sessionid=alpha");
                     req.response().headers().add("Set-Cookie", "auth-user=" + email.toString());
                     req.response().headers().add("Set-Cookie", "followed-teams=" + strings.toString());
                     req.response().sendFile(webroot + REPORTS_PAGE);
                     logger.info(email.toString() + " followed teams: " + teams);
                   }
                 }
               }, new Action1<Throwable>() {
                 @Override
                 public void call(Throwable throwable) {
                   logger.info("Error:" + throwable.getCause().getMessage());
                 }
               }
              );
            }
          }
        }
      }
    },
    new Action1<Throwable>() {
      public void call(Throwable err) {
        logger.info(err.getMessage());
      }
    });

    server.coreHttpServer().listen(port);
  }

  */
/***
   * Recursive search team by id
   * @param ob
   * @param teamIds
   * @param rxEventBus
   * @return
   *//*

  private Observable<RxMessage<JsonObject>> loop(Observable<RxMessage<JsonObject>> ob,
                                                 Iterator<String> teamIds, RxEventBus rxEventBus) {
    if (teamIds.hasNext())
      return loop (Observable.merge(ob, searchSingle(createTeamQuery(teamIds), rxEventBus)), teamIds, rxEventBus);
    else
      return ob;
  }

  private Observable<RxMessage<JsonObject>> searchSingle(JsonObject query, RxEventBus rxEventBus) {
    logger.info(query);
    return rxEventBus.<JsonObject, JsonObject>send(MONGO_MODULE_NAME, query);
  }

  private static String hash(String password) {
    return Hashing.md5().newHasher().putString(password, Charsets.UTF_8).hash().toString();
  }

  private void initPersistor(JsonObject config) {
    container.deployModule(VERTIX_MONGO_MODULE_NAME, config.getObject(MONGO_MODULE_NAME), 1, new AsyncResultHandler<String>() {
      @Override
      public void handle(AsyncResult<String> asyncResult) {
        if (asyncResult.failed()) {
          logger.info(asyncResult.cause());
        }
        logger.info(VERTIX_MONGO_MODULE_NAME + " say " + asyncResult.result());
      }
    });

    */
/*container.deployModule( VERTIX_JDBC_MODULE_NAME, config.getObject( JDBS_MODULE_NAME ), 1, new AsyncResultHandler<String>()
    {
      @Override
      public void handle( AsyncResult<String> asyncResult )
      {
        if ( asyncResult.failed() )
        {
          logger.info( asyncResult.cause() );
        }
        logger.info( VERTIX_JDBC_MODULE_NAME + " say " + asyncResult.result() );
      }
    } );*//*

  }

  private void initEventBus() {
    vertx.eventBus().registerHandler(EVENT_BUS_NAME, new Handler<Message<String>>() {
      @Override
      public void handle(Message<String> message) {
        logger.info(EVENT_BUS_NAME + "from " + message.replyAddress() + " Body: " + message.body());
        message.reply("pong!");
      }
    });
  }
}
*/
