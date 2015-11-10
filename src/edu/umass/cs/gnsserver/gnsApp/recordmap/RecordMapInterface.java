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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.recordmap;

//import edu.umass.cs.gnsserver.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.RecordExistsException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 *
 * @author westy, abhigyan
 */
public interface RecordMapInterface {

  /**
   * Add a name record to the database.
   * 
   * @param recordEntry
   * @throws FailedDBOperationException
   * @throws RecordExistsException
   */
  public void addNameRecord(NameRecord recordEntry) throws FailedDBOperationException, RecordExistsException;

  /**
   * Retrieve a name record from the database.
   * 
   * @param name
   * @return a NameRecord
   * @throws RecordNotFoundException
   * @throws FailedDBOperationException
   */
  public NameRecord getNameRecord(String name) throws RecordNotFoundException, FailedDBOperationException;

  /**
   * Update a name record in the database.
   * 
   * @param recordEntry
   * @throws FailedDBOperationException
   */
  public void updateNameRecord(NameRecord recordEntry) throws FailedDBOperationException;

  /**
   * Add a name record to the database from a JSON Object.
   * 
   * @param json
   * @throws FailedDBOperationException
   * @throws RecordExistsException
   */
  public void addNameRecord(JSONObject json) throws FailedDBOperationException, RecordExistsException;

  /**
   * Add multiple name records to the database from an list of JSON Objects.
   * 
   * @param jsons
   * @throws FailedDBOperationException
   * @throws RecordExistsException
   */
  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws FailedDBOperationException, RecordExistsException;

  /**
   * Remove a name record from the database.
   * 
   * @param name
   * @throws FailedDBOperationException
   */
  public void removeNameRecord(String name) throws FailedDBOperationException;

  /**
   * Returns true if the database contains a name record with the given name.
   * 
   * @param name
   * @return true if the database contains a name record with the given name
   * @throws FailedDBOperationException
   */
  public boolean containsName(String name) throws FailedDBOperationException;

  /**
   * Return all the column keys from a name record in the database.
   * 
   * @param key
   * @return a set of strings
   * @throws RecordNotFoundException
   * @throws FailedDBOperationException
   */
  public Set<String> getAllColumnKeys(String key) throws RecordNotFoundException, FailedDBOperationException;

  /**
   * Clears the database and reinitializes all indices.
   * 
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public void reset() throws FailedDBOperationException;

  /**
   * Retrieves all the system fields from the database that match field.
   * 
   * @param name - the name of the record
   * @param nameField - the field that contains the name of the record
   * @param fields - the system fields to retrieve
   * @return a map of {@link ColumnField} to objects
   * @throws FailedDBOperationException
   * @throws RecordNotFoundException
   */
  public HashMap<ColumnField, Object> lookupMultipleSystemFields(String name, ColumnField nameField, ArrayList<ColumnField> fields) throws
          FailedDBOperationException, RecordNotFoundException;

  /**
   *
   * @param name  - the name of the record
   * @param nameField - the field that contains the name of the record
   * @param fields - the system fields to retrieve
   * @param valuesMapField - the field that contains all the user fields
   * @param valuesMapKeys - the user fields to return
   * @return a map of {@link ColumnField} to objects
   * @throws FailedDBOperationException
   * @throws RecordNotFoundException
   */
  public HashMap<ColumnField, Object> lookupMultipleSystemAndUserFields(String name, ColumnField nameField, ArrayList<ColumnField> fields,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws
          FailedDBOperationException, RecordNotFoundException;

  /**
   * Update system fields in a record.
   * 
   * @param name - the name of the record
   * @param nameField - the field that contains the name of the record
   * @param fields - the system fields to update
   * @param values - the values to set them to
   * @throws FailedDBOperationException
   */
  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields, ArrayList<Object> values)
          throws FailedDBOperationException;

  /**
   * Update system and user fields in a record.
   * 
   * @param name - the name of the record
   * @param nameField - the field that contains the name of the record
   * @param fields - the system fields to update
   * @param values - the values to set the system fields to
   * @param valuesMapField - the field that contains all the user fields
   * @param valuesMapKeys - the user fields to update
   * @param valuesMapValues - the values to set the user fields to to
   * @throws FailedDBOperationException
   */
  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields, ArrayList<Object> values,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException;

  /**
   * Updates the record indexed by name conditionally. The condition specified by 
   * conditionField whose value must be equal to conditionValue.
   * 
   * @param name - the name of the record
   * @param nameField - the field that contains the name of the record
   * @param conditionField
   * @param conditionValue
   * @param fields - the system fields to update
   * @param values - the values to set the system fields to
   * @param valuesMapField - the field that contains all the user fields
   * @param valuesMapKeys - the user fields to update
   * @param valuesMapValues - the values to set the user fields to to
   * @return Returns true if the update was applied false otherwise.
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public abstract boolean updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue,
          ArrayList<ColumnField> fields, ArrayList<Object> values, ColumnField valuesMapField,
          ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException;

  /**
   * Remove keys from a field.
   * 
   * @param name - the name of the record
   * @param mapField - the system fields to update
   * @param mapKeys - the keys to remove
   * @throws FailedDBOperationException
   */
  public abstract void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException;

  /**
   * Returns an iterator for all the rows in the collection with only the columns in fields filled in except
   * the NAME (AKA the primary key) is always there.
   *
   * @param nameField - the field in the row that contains the name field
   * @param fields
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields) throws FailedDBOperationException;

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor getAllRowsIterator() throws FailedDBOperationException;

  /**
   * Given a key and a value return all the records as a AbstractRecordCursor that 
   * have a *user* key with that value.
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified 
   * as a nested JSONArray string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]].
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified 
   * as a JSONArray string tuple: [LONG, LAT]. maxDistance is in radians. 
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [LONG, LAT]
   * @param maxDistance - the distance in meters
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException;

  /**
   * Return all the fields that match the query.
   * 
   * @param valuesMapField
   * @param query
   * @return {@link AbstractRecordCursor}
   * @throws FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) throws FailedDBOperationException;

}
