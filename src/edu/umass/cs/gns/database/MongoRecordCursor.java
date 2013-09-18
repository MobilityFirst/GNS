/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import edu.umass.cs.gns.exceptions.GnsRuntimeException;
import edu.umass.cs.gns.main.GNS;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides a cursor that can be used to iterate through rows of a collection.
 * Currently can generate rows as HashMaps or JSONObjects (which are really the same structure anyway)
 * and can populate all columns (fields) or just some of the columns in the row.
 * 
 * @author westy
 */
public class MongoRecordCursor extends BasicRecordCursor {

  private Field nameField;
  private ArrayList<Field> fields;
  private DBCursor cursor;
  private boolean allFields = false;

  /**
   * Returns a cursor that iterates through all the rows in a collection.
   * @param db
   * @param collectionName
   * @param nameField 
   */
  public MongoRecordCursor(DB db, String collectionName, Field nameField) {
    this.nameField = nameField;
    this.allFields = true;

    db.requestEnsureConnection();
    DBCollection collection = db.getCollection(collectionName);
    // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
    BasicDBObject query = new BasicDBObject(nameField.getName(), new BasicDBObject("$exists", true));

    cursor = collection.find(query);
  }

  public MongoRecordCursor(DB db, String collectionName, Field nameField, ArrayList<Field> fields) {
    this.nameField = nameField;
    this.fields = fields;
    this.allFields = false;

    db.requestEnsureConnection();
    DBCollection collection = db.getCollection(collectionName);
    // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
    BasicDBObject query = new BasicDBObject(nameField.getName(), new BasicDBObject("$exists", true));
    BasicDBObject projection = new BasicDBObject().append("_id", 0);
    projection.append(nameField.getName(), 1); // name field must be returned.
    if (fields != null) { // add other fields requested
      for (Field f : fields) {
        projection.append(f.getName(), 1);
      }
    }
    cursor = collection.find(query, projection);
  }
  
  /**
   * Returns a cursor that iterates through all the rows in a collection.
   * @param db
   * @param collectionName
   * @param nameField 
   */
  public MongoRecordCursor(DBCursor cursor, Field nameField) {
    this.nameField = nameField;
    this.cursor = cursor;
    this.allFields = true;
  }

  private JSONObject nextAllFieldsJSON() {
    if (cursor.hasNext()) {
      DBObject dbObject = cursor.next();
      try {
        return new JSONObject(dbObject.toString());
      } catch (JSONException e) {
        // Since next can't throw anything which isn't a runtime exception.
        // replace these with not a runtime exception?
        throw new GnsRuntimeException("Error parsing JSON object.");
      }
    } else {
      // replace these with not a runtime exception?
      throw new NoSuchElementException();
    }
  }

  private HashMap<Field, Object> nextSomeFieldsHashMap() {
    if (cursor.hasNext()) {
      HashMap<Field, Object> hashMap = new HashMap<Field, Object>();
      DBObject dbObject = cursor.next();

      hashMap.put(nameField, dbObject.get(nameField.getName()).toString());// put the name in the hashmap!! very important!!

      FieldType.populateHashMap(hashMap, dbObject, fields); // populate other fields in hashmap

      return hashMap;
    } else {
      // replace these with not a runtime exception?
      throw new NoSuchElementException();
    }
  }

  /**
   * Returns the next row as a JSONObject.
   * 
   * @return 
   */
  @Override
  public JSONObject nextJSONObject() {
    if (allFields) {
      return nextAllFieldsJSON();
    } else {
      return hashMapWithFieldsToJSONObject(nextSomeFieldsHashMap());
    }
  }

  /**
   * Returns the next row as a HashMap.
   * 
   * @return 
   */
  @Override
  public HashMap<Field, Object> nextHashMap() {
    if (allFields) {
      throw new UnsupportedOperationException("Not supported yet.");
    } else {
      return nextSomeFieldsHashMap();
    }
  }
  
  public String nextRowField(String name) {
    if (cursor.hasNext()) {
      DBObject dbObject = cursor.next();
      try {
        // capitalizing on the fact that we know dbObjects are really JSONObjects
        return new JSONObject(dbObject.toString()).getString(name);
      } catch (JSONException e) {
        throw new GnsRuntimeException("Error parsing JSON object.");
      }
    } else {
      // replace these with not a runtime exception?
      throw new NoSuchElementException();
    }
  }

  @Override
  public boolean hasNext() {
    return cursor.hasNext();
  }

  @Override
  public JSONObject next() {
    return nextJSONObject();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported.");
  }

  private JSONObject hashMapWithFieldsToJSONObject(HashMap<Field, Object> map) {
    JSONObject json = new JSONObject();
    for (Entry<Field, Object> entry : map.entrySet()) {
      try {
        json.put(entry.getKey().getName(), entry.getValue());
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem putting object in JSONObject: " + e);
      }
    }
    return json;
  }
}
