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

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Provides an interface for insert, updateEntireRecord, remove and lookup operations in a nosql database.
 *
 * @author Westy
 */
public interface NoSQLRecords {

  /**
   * Create a new record (row) with the given name using the JSONObject.
   *
   * @param collection collection to be inserted into
   * @param name the name of the record record name to be inserted
   * @param value value to be inserted (a JSONObject)
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   * @throws edu.umass.cs.gnscommon.exceptions.server.RecordExistsException
   */
  public void insert(String collection, String name, JSONObject value)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException,
          edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;

  /**
   * For the record with given name, return the entire record as a JSONObject.
   *
   * @param collection the name of the collection
   * @param name the name of the record of the record
   * @return a JSONObject
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   * @throws RecordNotFoundException
   */
  public JSONObject lookupEntireRecord(String collection, String name)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException,
          edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;

  /**
   * For record with given name, return the values of given fields and from the values map field of the record,
   * return the values of given keys as a HashMap.
   *
   * @param collectionName
   * @param guid
   * @param nameField
   * @param valuesMapField
   * @param valuesMapKeys
   * @return a hashmap of ColumnField to Objects
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   * @throws edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException
   */
  public HashMap<ColumnField, Object> lookupSomeFields(String collectionName,
          String guid, ColumnField nameField, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException;

  /**
   * Returns true if a record with the given name exists, false otherwise.
   *
   * @param collection the name of the collection
   * @param name the name of the record
   * @return true if the record exists
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public boolean contains(String collection, String name) throws
          edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * Remove the record with the given name.
   *
   * @param collection the name of the collection
   * @param name the name of the record
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public void removeEntireRecord(String collection, String name) throws
          edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * For the record with given name, replace the values of given fields to the given values.
   *
   * @param collection the name of the collection
   * @param name the name of the record
   * @param values
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public abstract void updateEntireRecord(String collection, String name,
          ArrayList<Object> values) throws
          edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * THE ONLY METHOD THAT CURRENTLY SUPPORTS WRITING USER JSON OBJECTS AS VALUES IN THE VALUES MAP.
   * ALSO SUPPORTS DOT NOTATION.
   *
   * @param collectionName
   * @param guid
   * @param valuesMapField
   * @param valuesMapKeys
   * @param valuesMapValues
   * @throws FailedDBOperationException
   */
  public void updateIndividualFields(String collectionName, String guid,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException;

  /**
   * For record with name, removes (unset) keys in list <code>mapKeys</code> from the map <code>mapField</code>.
   *
   * @param collectionName the name of the collection
   * @param name the name of the record
   * @param mapField the field that contains the keys to remove
   * @param mapKeys the keys to remove
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public void removeMapKeys(String collectionName, String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @param collection
   * @return an AbstractRecordCursor
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public AbstractRecordCursor getAllRowsIterator(String collection)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * Given a key and a value return all the records as a AbstractRecordCursor that have a *user* key with that value.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @return AbstractRecordCursor
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public AbstractRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * If key is a GeoSpatial field returns all guids that are within value which is a bounding box specified as a nested JSONArray
   * string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a AbstractRecordCursor.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @return AbstractRecordCursor
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public AbstractRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * If key is a GeoSpatial field returns all guids that are near value which is a point specified as a JSONArray string tuple:
   * [LONG, LAT]. maxDistance is in meters. The returned value is a {@link AbstractRecordCursor}.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @param maxDistance
   * @return an AbstractRecordCursor
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public AbstractRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value, Double maxDistance)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * Performs a query on the database and returns all guids that satisfy the query.
   * The returned value is a {@link AbstractRecordCursor}.
   *
   * @param collection the name of the collection
   * @param valuesMapField the field that contains the ValuesMap
   * @param query the query to execute
   * @return an AbstractRecordCursor
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public AbstractRecordCursor selectRecordsQuery(String collection, ColumnField valuesMapField, String query)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

  /**
   * Creates an index for the given field.
   *
   * @param collectionName
   * @param field
   * @param index
   */
  public void createIndex(String collectionName, String field, String index);

  /**
   * Return a string representation of the record set.
   *
   * @return a string
   */
  @Override
  public String toString();

  /**
   * Print all the records (ONLY FOR DEBUGGING).
   * You've been warned!
   *
   * @param collection the name of the collection
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public void printAllEntries(String collection) throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

}
