package com.gateway.server.test.unit;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.*;
import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import rx.Observable;
import rx.util.functions.Action1;
import rx.util.functions.Func2;
import rx.util.functions.FuncN;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExampleUnitTest {

  /*@Test
  public void testVerticle() {
    SportPlanetVerticle vert = new SportPlanetVerticle();

    // Interrogate your classes directly....
    assertNotNull(vert);
  }*/


  /*@Test
  public void testMongo() throws UnknownHostException {
    final MongoClient mongo = new MongoClient("localhost", 27017);

    final DB db = mongo.getDB("test_db");

    final DBCollection collection = db.getCollection("test");
    final String[] ids = {"39d6d76f-8572-4a33-a598-bc7dda1da180", "39d6d76f-8572-4a33-a598-bc7dda1da181"};

    double d = 56.78;

    float f = 56.78f;

    Float fl = 6f;

    Map<String, Object> q = ImmutableMap.<String, Object>of(
        "fnum", f );

        //"_id", new BasicDBObject("$in", new String[] { "52be80044ac32b424d50bc5b", "52be80044ac32b424d50bc5c" } ));

        //"num", new BasicDBObject("$gt", 5).append("$lt", 57));
        *//*,
        "num2", new BasicDBObject("$gt", 99),
        "name", "Aaa");*//*

    BasicDBObject query = new BasicDBObject(q);

        *//*"num", new BasicDBObject("$gt", 5).append("$lt", 10))
        .append("name", "Aaa");*//*


    //BasicDBObject query = new BasicDBObject("num", new BasicDBObject("$gt", 5)).append("$lt", 10);

    System.out.println(query);
    final DBCursor cursor = collection.find(query);

    while(cursor.hasNext()) {
      System.out.println(cursor.next());
    }

    cursor.close();

    *//*JsonArray ids1 = new JsonArray();
    ids1.addString(ids[0]);
    ids1.addString(ids[1]);

    JsonObject matcher = new JsonObject();
    matcher.putString("query", "field1 $gt 5 $lt 10 and field1 $in [\"1\",\"2\"]");

    JsonObject q = new JsonObject()
        .putString("collection", "teams")
        .putString("action", "find")
        .putObject("matcher", matcher);


    //System.out.println(matcher.toMap());

    for (Object o: matcher.toMap().values()) {
      if (Map.class.isAssignableFrom(o.getClass())) {
        Map casted = (Map)o;
        System.out.println(casted.values());
      } else {
        System.out.println(o);
      }
    }

    final DBCursor cursor =  collection.find(jsonToDBObject(matcher));
    while(cursor.hasNext()) {
      System.out.println(cursor.next());
    }

    cursor.close();*//*
  }*/

  private DBObject jsonToDBObject(JsonObject object) {
    return new BasicDBObject(object.toMap());
  }

  @Test
  public void testMerge() {

    Observable<Integer> f = Observable.from(1);
    Observable<Integer> s = Observable.from(2);
    Observable<Integer> t = Observable.from(3);
    Observable<Integer> h = Observable.from(4);


    Observable.merge(f, s, t, h).aggregate(null, new Func2<String, Integer, String>() {
      @Override
      public String call(String acc, Integer current) {
        System.out.println(acc + " " + current);
        return Joiner.on(',').skipNulls().join(acc, current);
      }
    }).subscribe(new Action1<String>() {
      @Override
      public void call(String s) {
        System.out.println(s);
      }
    });

  }


  @Test
  public void testAmb() {

    final Observable<Integer> f = Observable.from(1);
    final Observable<Integer> s = Observable.from(2);

    Observable.amb(s, f).subscribe(new Action1<Integer>() {
      @Override
      public void call(Integer integer) {
       System.out.println(integer);
      }
    });
  }

  @Test
  public void testZip() {

    List<Observable<Integer>> list = Arrays.asList(Observable.from(1), Observable.from(2), Observable.from(3), Observable.from(4));

    Observable.zip(list, new FuncN<Map<Integer, Integer>>() {
      @Override
      public Map<Integer, Integer> call(Object... objects) {

        Integer[] array = new Integer[objects.length];
        System.arraycopy(objects, 0, array, 0, objects.length);
        Map<Integer, Integer> result = new HashMap<>();
        int key = 0;
        for (Integer current : array) {
          result.put(key++ , current);
        }

        return result;
      }
    }).subscribe(new Action1<Map<Integer, Integer>>() {
      @Override
      public void call(Map<Integer, Integer> s) {
        System.out.println(s);
      }
    });

  }
}
