package org.apache.carbondata.examples.gson;

import java.io.IOException;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

public class NewObjAdapter extends TypeAdapter {

  @Override
  public void write(JsonWriter jsonWriter, Object o) throws IOException {

  }

  @Override
  public Object read(JsonReader jsonReader) throws IOException {
    NewObj newObj = new NewObj();
    jsonReader.beginObject();
    while (jsonReader.hasNext()) {
      switch (jsonReader.nextName()) {
        case "a":
          newObj.a = jsonReader.nextString();
          break;
        case "b":
          NewValue newValue = new NewValue();
          newValue.newValue1 = jsonReader.nextString();
          newObj.b = newValue;
          break;
      }
    }
    jsonReader.endObject();
    return newObj;
  }
}