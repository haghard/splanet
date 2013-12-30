import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.platform.Verticle;

public class HttpClient extends Verticle
{
  @Override
  public void start()
  {
    vertx.createHttpClient().setHost( "192.168.0.143" ).setPort( 9000 ).getNow( "/recent?followedTeams=MU,CH", new  Handler<HttpClientResponse>() {

      public void handle(HttpClientResponse response) {
        response.bodyHandler(new Handler<Buffer>() {
          public void handle(Buffer data) {
            System.out.println(data);
          }
        });
      }
    });

  }
}
