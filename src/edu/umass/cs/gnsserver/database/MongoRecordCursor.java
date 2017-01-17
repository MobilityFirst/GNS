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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.ServerRuntimeException;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.NoSuchElementException;

/**
 * Provides a cursor that can be used to iterate through rows of a collection.
 * Currently can generate rows as HashMaps or JSONObjects (which are really the same structure anyway)
 * and can populate all columns (fields) or just some of the columns in the row.
 *
 * @author westy
 */
public class MongoRecordCursor extends AbstractRecordCursor {

  private DBCursor cursor;

  /**
   * Returns a cursor that iterates through all the rows in a collection and includes all columns.
   *
   * @param db
   * @param collectionName
   * @param nameField
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public MongoRecordCursor(DB db, String collectionName, ColumnField nameField) throws FailedDBOperationException {
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(nameField.getName(), new BasicDBObject("$exists", true));

      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, nameField.toString(), 
              "Original mongo exception:" + e.getMessage());
    }
  }

  /**
   * Wraps a cursor around the given DBCursor that iterates through all the rows indexed by the cursor.
   *
   * @param cursor
   * @param nameField
   */
  public MongoRecordCursor(DBCursor cursor, ColumnField nameField) {
    this.cursor = cursor;
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
          throw new ServerRuntimeException("Error parsing JSON object.");
        }
      } else {
        // replace these with not a runtime exception?
        throw new NoSuchElementException();
      }
    } catch (MongoException e) {
      throw new FailedDBOperationException(cursor.getCollection().getName(), cursor.toString(),
              "Original mongo exception:" + e.getMessage());
    }
  }

  /**
   * Returns the next row as a JSONObject.
   *
   * @return the next row as a JSONObject
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  @Override
  public JSONObject nextJSONObject() throws FailedDBOperationException {
    return nextAllFieldsJSON();
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
        throw new ServerRuntimeException("Error parsing JSON object.");
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  @Override
  public boolean hasNext() throws FailedDBOperationException {
    try {
      return cursor.hasNext();
    } catch (MongoException e) {
      throw new FailedDBOperationException(cursor.getCollection().getName(), cursor.toString(),
              "Original mongo exception:" + e.getMessage());
    }
  }
}
