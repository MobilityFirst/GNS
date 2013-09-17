/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.util.NoSuchElementException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class MongoRecordCursor implements BasicRecordCursor {

  public MongoRecordCursor(DBCursor cursor) {
    this.cursor = cursor;
  }
  private DBCursor cursor;

  @Override
  public boolean hasNext() {
    return cursor.hasNext();
  }

  @Override
  public JSONObject next() {
    if (cursor.hasNext()) {
      DBObject dbObject = cursor.next();
      try {
        return new JSONObject(dbObject.toString());
      } catch (JSONException e) {
        // Since next can't throw anything which isn't a runtime exception.
        throw new RuntimeException("Error parsing JSON object.");
      }
    } else {
      throw new NoSuchElementException();
    }

  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported.");
  }
}
