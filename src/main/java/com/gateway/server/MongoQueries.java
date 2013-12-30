package com.gateway.server;

import org.vertx.java.core.json.JsonObject;

import java.util.Iterator;

public final class MongoQueries {

  public static JsonObject createTeamQuery(Iterator<String> ids) {
    final StringBuilder strings = new StringBuilder();
    boolean first = true;
    while (ids.hasNext()) {
      if (first) {
        strings.append("\"").append(ids.next()).append("\"");
        first = false;
      } else {
        strings.append(',').append("\"").append(ids.next()).append("\"");
      }
    }

    return new JsonObject().putString("collection", "teams")
        .putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " _id $in { " + strings.toString() + " }" ));
  }

  public static JsonObject createRecentIdsQuery(Iterator<String> teamNames) {
    return new JsonObject().putString("collection", "recent")
        .putString("action", "find")
        .putObject("matcher",
            new JsonObject().putString("teamName", teamNames.next()));
  }


  public static JsonObject createRecentResultsQuery(Iterator<Object> teamNames) {
    return new JsonObject().putString("collection", "results")
        .putString("action", "find")
        .putObject("matcher",
            new JsonObject().putString("_id", teamNames.next().toString()));
  }

  public static JsonObject createRecentResultsQuery0(Iterator<Object> teamNames) {
    final StringBuilder ids = new StringBuilder();
    boolean first = true;
    while (teamNames.hasNext()) {
      if (first) {
        ids.append("\"").append(teamNames.next().toString()).append("\"");
        first = false;
      } else {
        ids.append(',').append("\"").append(teamNames.next().toString()).append("\"");
      }
    }

    return new JsonObject().putString("collection", "results")
        .putString("action", "find")
        .putObject("matcher",
            new JsonObject()
                .putString("query", "_id $in { " + ids.toString() + " }"));
  }

  public static JsonObject createUserQuery(String passwordHash, String email) {
    return new JsonObject().putString("collection", "users")
        .putString("action", "find")
        .putObject("matcher", new JsonObject()
            .putString("query", " email $eq \"" + email.toString() + "\"" + " password $eq \"" + passwordHash + "\""));
  }
}