package com.gateway.server.test.unit;

import com.google.common.base.Joiner;
import org.junit.Test;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func2;
import rx.functions.FuncN;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExampleUnitTest {

  @Test
  public void testReduce() {
    Observable.from(new Integer[] {1,2,3,4}).reduce(new Func2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer f, Integer s) {
        return f + s;
      }
    }).subscribe(new Action1<Integer>() {
      @Override public void call(Integer s) {
        System.out.println("reduce " + s);
      }
    });

    System.out.println(new JsonObject().mergeIn(new JsonObject().putString("b", "2")));
    System.out.println(new JsonObject().putString("b", "2").mergeIn(new JsonObject()));
    System.out.println(new JsonObject().putString("a", "1").mergeIn(new JsonObject().putString("a", "2")));
  }

  @Test
  public void testMerge() {

    Observable<Integer> f = Observable.from(1);
    Observable<Integer> s = Observable.from(2);
    Observable<Integer> t = Observable.from(3);
    Observable<Integer> h = Observable.from(4);

    Observable.merge(f, s, t, h).reduce(null, new Func2<String, Integer, String>() {
      @Override
      public String call(String acc, Integer current) {
        System.out.println(acc + " " + current);
        return Joiner.on(',').skipNulls().join(acc, current);
      }
    }).subscribe(new Action1<String>() {
      @Override
      public void call(String s) {
        System.out.println("merge " + s);
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
       System.out.println("amb: " + integer);
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
          result.put(key++, current);
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

  @Test
  public void testMergeJsonArray() {
    JsonArray a = new JsonArray()
        .addObject(new JsonObject().putNumber("num", 1).putString("val", "Aaa"))
        .addObject(new JsonObject().putNumber("num", 2).putString("val", "Bbb"));


    JsonArray b = new JsonArray()
        .addObject(new JsonObject().putNumber("num", 3).putString("val", "Ccc"))
        .addObject(new JsonObject().putNumber("num", 4).putString("val", "Ddd"));

    JsonArray c = a.addArray(b);

    System.out.println(c.toString().replaceAll("\\[|]",""));
    System.out.println(c);
  }
}
