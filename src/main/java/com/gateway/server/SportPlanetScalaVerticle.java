package com.gateway.server;

import io.vertx.rxcore.java.eventbus.RxEventBus;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

/**
 *
 *
 */
public class SportPlanetScalaVerticle extends Verticle {

  private Logger logger;

  public static final String MONGO_MODULE_NAME = "mongo-persistor";
  public static final String VERTIX_MONGO_MODULE_NAME = "io.vertx~mod-mongo-persistor~2.1.1";

  public void start() {

    logger = container.logger();

    int port = container.config().getObject("network-settings").getInteger("port");

    deployMongoPersistor(container.config().getObject(MONGO_MODULE_NAME));

    final RxEventBus rxEventBus = new RxEventBus(vertx.eventBus());
    new Application(vertx.createHttpServer(), rxEventBus,
        container.config().getObject(MONGO_MODULE_NAME),
        container.logger(),
        port).start();
  }

  private void deployMongoPersistor(JsonObject config) {
    container.deployModule(VERTIX_MONGO_MODULE_NAME, config, 1, new AsyncResultHandler<String>() {
      @Override
      public void handle(AsyncResult<String> asyncResult) {
      if (asyncResult.failed()) {
        logger.info("Error: " + asyncResult.cause());
      } else {
        logger.info(VERTIX_MONGO_MODULE_NAME + " say " + asyncResult.result());
      }
      }
    });
  }
}