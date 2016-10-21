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
package edu.umass.cs.gnsserver.gnsapp.recordmap;

//import edu.umass.cs.gnsserver.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author Westy
 */
public interface RecordMapInterface {

  /**
   * Creates an index for the field.
   *
   * @param field
   * @param index
   */
  public void createIndex(String field, String index);

  /**
   * Add a name record to the database from a JSON Object.
   *
   * @param json
   * @throws FailedDBOperationException
   * @throws RecordExistsException
   */
  public void addRecord(JSONObject json) throws FailedDBOperationException, RecordExistsException;

  /**
   * Retrieve a name record from the database.
   *
   * @param name
   * @return a NameRecord
   * @throws RecordNotFoundException
   * @throws FailedDBOperationException
   */
  public JSONObject lookupEntireRecord(String name) throws RecordNotFoundException, FailedDBOperationException;

  /**
   *
   * @param name - the name of the record
   * @param nameField - the field that contains the name of the record
   * @param valuesMapField - the field that contains all the user fields
   * @param valuesMapKeys - the user fields to return
   * @return a map of {@link ColumnField} to objects
   * @throws FailedDBOperationException
   * @throws RecordNotFoundException
   */
  public HashMap<ColumnField, Object> lookupUserFields(String name, ColumnField nameField,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException;

  /**
   * Remove a name record from the database.
   *
   * @param name
   * @throws FailedDBOperationException
   */
  public void removeRecord(String name) throws FailedDBOperationException;

  /**
   * Returns true if the database contains a name record with the given name.
   *
   * @param name
   * @return true if the database contains a name record with the given name
   * @throws FailedDBOperationException
   */
  public boolean containsName(String name) throws FailedDBOperationException;

//   /**
//   *
//   * @param name  - the name of the record
//   * @param nameField - the field that contains the name of the record
//   * @param systemFields
//   * @return a map of {@link ColumnField} to objects
//   * @throws FailedDBOperationException
//   * @throws RecordNotFoundException
//   */
//   public HashMap<ColumnField, Object> lookupSystemFields(String name, ColumnField nameField, 
//          ArrayList<ColumnField> systemFields) throws RecordNotFoundException, FailedDBOperationException;
//   
  /**
   * Update all fields in a record.
   *
   * @param name - the name of the record
   * @param valuesMap
   * @throws FailedDBOperationException
   */
  public abstract void updateEntireValuesMap(String name, ValuesMap valuesMap)
          throws FailedDBOperationException;

  /**
   * Update particular fields in a record.
   *
   * @param name - the name of the record
   * @param valuesMapKeys - the user fields to update
   * @param valuesMapValues - the values to set the user fields to to
   * @throws FailedDBOperationException
   */
  public abstract void updateIndividualFields(String name, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
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
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecords(ColumnField valuesMapField,
          String key, Object value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified
   * as a nested JSONArray string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]].
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsWithin(ColumnField valuesMapField,
          String key, String value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified
   * as a JSONArray string tuple: [LONG, LAT]. maxDistance is in radians.
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [LONG, LAT]
   * @param maxDistance - the distance in meters
   * @return an {@link AbstractRecordCursor}
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsNear(ColumnField valuesMapField,
          String key, String value, Double maxDistance) throws FailedDBOperationException;

  /**
   * Return all the fields that match the query.
   *
   * @param valuesMapField
   * @param query
   * @return {@link AbstractRecordCursor}
   * @throws FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsQuery(ColumnField valuesMapField,
          String query) throws FailedDBOperationException;

}
