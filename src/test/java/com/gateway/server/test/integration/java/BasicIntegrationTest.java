package com.gateway.server.test.integration.java;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.*;

/**
 * Simple integration test which shows tests deploying other verticles, using the Vert.x API etc
 */
public class BasicIntegrationTest extends TestVerticle
{

  @Test
  public void testHTTP()
  {
    // Create an HTTP server which just sends back OK response immediately
    vertx.createHttpServer().requestHandler( new Handler<HttpServerRequest>()
    {
      public void handle( HttpServerRequest req )
      {
        req.response().end();
      }
    } ).listen( 8182, new AsyncResultHandler<HttpServer>()
    {
      @Override
      public void handle( AsyncResult<HttpServer> asyncResult )
      {
        assertTrue( asyncResult.succeeded() );
        // The server is listening so send an HTTP request
        vertx.createHttpClient().setPort( 8182 ).getNow( "/", new Handler<HttpClientResponse>()
        {
          @Override
          public void handle( HttpClientResponse resp )
          {
            assertEquals( 200, resp.statusCode() );
            /*
            If we get here, the test is complete
            You must always call `testComplete()` at the end. Remember that testing is *asynchronous* so
            we cannot assume the test is complete by the time the test method has finished executing like
            in standard synchronous tests
            */
            testComplete();
          }
        } );
      }
    } );
  }

  @Test
  public void testCompleteOnTimer()
  {
    vertx.setTimer( 1000, new Handler<Long>()
    {
      @Override
      public void handle( Long timerID )
      {
        assertNotNull( timerID );

        // This demonstrates how tests are asynchronous - the timer does not fire until 1 second later -
        // which is almost certainly after the test method has completed.
        testComplete();
      }
    } );
  }
}
