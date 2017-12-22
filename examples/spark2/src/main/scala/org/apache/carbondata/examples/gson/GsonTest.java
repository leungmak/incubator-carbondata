package org.apache.carbondata.examples.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonTest {
  public static void main(String[] args) {
    String obj = "{'a'='fieldA', 'b'='fieldB'}";
    Gson gson = new Gson();
    OldObj oldObj = gson.fromJson(obj, OldObj.class);
    System.out.println(oldObj);

    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(NewObj.class, new NewObjAdapter());
    Gson gson2 = builder.create();
    NewObj newObj = gson2.fromJson(obj, NewObj.class);
    System.out.println(newObj);
  }

}
