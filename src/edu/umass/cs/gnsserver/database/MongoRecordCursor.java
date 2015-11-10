/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.database;

import com.mongodb.*;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.GnsRuntimeException;
import edu.umass.cs.gnsserver.main.GNS;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * Provides a cursor that can be used to iterate through rows of a collection.
 * Currently can generate rows as HashMaps or JSONObjects (which are really the same structure anyway)
 * and can populate all columns (fields) or just some of the columns in the row.
 *
 * @author westy
 */
public class MongoRecordCursor extends AbstractRecordCursor {

  private ColumnField nameField;
  private ArrayList<ColumnField> fields;
  private DBCursor cursor;
  private boolean allFields = false;

  /**
   * Returns a cursor that iterates through all the rows in a collection and includes all columns.
   *
   * @param db
   * @param collectionName
   * @param nameField
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public MongoRecordCursor(DB db, String collectionName, ColumnField nameField) throws FailedDBOperationException {
    this.nameField = nameField;
    this.allFields = true;
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(nameField.getName(), new BasicDBObject("$exists", true));

      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, nameField.toString());
    }
  }

  /**
   * Returns a cursor that iterates through all the rows in a collection and includes all columns in fields.
   *
   * @param db
   * @param collectionName
   * @param nameField
   * @param fields
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException 
   */
  public MongoRecordCursor(DB db, String collectionName, ColumnField nameField, ArrayList<ColumnField> fields)
          throws FailedDBOperationException {
    this.nameField = nameField;
    this.fields = fields;
    this.allFields = false;
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(nameField.getName(), new BasicDBObject("$exists", true));
      BasicDBObject projection = new BasicDBObject().append("_id", 0);
      projection.append(nameField.getName(), 1); // name field must be returned.
      if (fields != null) { // add other fields requested
        for (ColumnField f : fields) {
          projection.append(f.getName(), 1);
        }
      }
      cursor = collection.find(query, projection);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, nameField.toString());
    }
  }

  /**
   * Wraps a cursor around the given DBCursor that iterates through all the rows indexed by the cursor.
   *
   * @param cursor
   * @param nameField
   */
  public MongoRecordCursor(DBCursor cursor, ColumnField nameField) {
    this.nameField = nameField;
    this.cursor = cursor;
    this.allFields = true;
  }

  /**
   * Returns all the fields in the next row as a JSONObject.
   *
   * @return all the fields as a JSONObject
   */
  private JSONObject nextAllFieldsJSON() throws FailedDBOperationException {
    try {
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
    } catch (MongoException e) {
      throw new FailedDBOperationException(cursor.getCollection().getName(), cursor.toString());
    }
  }

  /**
   * Returns all the fields in the next row as a HashMap.
   *
   * @return the fields as a HashMap of ColumnFields and Objects
   */
  private HashMap<ColumnField, Object> nextSomeFieldsHashMap() {
    if (cursor.hasNext()) {
      HashMap<ColumnField, Object> hashMap = new HashMap<>();
      DBObject dbObject = cursor.next();

      hashMap.put(nameField, dbObject.get(nameField.getName()).toString());// put the name in the hashmap!! very important!!

      ColumnFieldType.populateHashMap(hashMap, dbObject, fields); // populate other fields in hashmap

      return hashMap;
    } else {
      // replace these with not a runtime exception?
      throw new NoSuchElementException();
    }
  }

  /**
   * Returns the next row as a JSONObject.
   *
   * @return the next row as a JSONObject
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  @Override
  public JSONObject nextJSONObject() throws FailedDBOperationException {
    if (allFields) {
      return nextAllFieldsJSON();
    } else {
      return hashMapWithFieldsToJSONObject(nextSomeFieldsHashMap());
    }
  }

  /**
   * Returns the next row as a HashMap.
   *
   * @return the next row as a HashMap
   */
  @Override
  public HashMap<ColumnField, Object> nextHashMap() {
    if (allFields) {
      throw new UnsupportedOperationException("Not supported yet.");
    } else {
      return nextSomeFieldsHashMap();
    }
  }

  /**
   * Returns the value of the field named name in the next row.
   *
   * @param name
   * @return the name of the field as a String
   */
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

  /**
   * Returns true if the iteration has more elements.
   * 
   * @return true or false
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  @Override
  public boolean hasNext() throws FailedDBOperationException {
    try {
      return cursor.hasNext();
    } catch (MongoException e) {
      throw new FailedDBOperationException(cursor.getCollection().getName(), cursor.toString());
    }
  }

  private JSONObject hashMapWithFieldsToJSONObject(HashMap<ColumnField, Object> map) throws FailedDBOperationException {
    try {
      JSONObject json = new JSONObject();
      for (Entry<ColumnField, Object> entry : map.entrySet()) {
        try {
          json.put(entry.getKey().getName(), entry.getValue());
        } catch (JSONException e) {
          GNS.getLogger().severe("Problem putting object in JSONObject: " + e);
        }
      }
      return json;
    } catch (MongoException e) {
      throw new FailedDBOperationException(cursor.getCollection().getName(), cursor.toString());
    }

  }
}
