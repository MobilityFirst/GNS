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

import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.NoSQLRecords;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;

/**
 * Supports abstract access to a collection of NoSQLRecords records specified by the
 * <code>collectionName</code> string.
 *
 * @author westy
 * @param <NodeIDType>
 */
public class GNSRecordMap<NodeIDType> extends BasicRecordMap {

  private final String collectionName;
  private final NoSQLRecords noSqlRecords;

  /**
   * Creates an MongoRecordMap instance.
   *
   * @param noSqlRecords
   * @param collectionName
   */
  public GNSRecordMap(NoSQLRecords noSqlRecords, String collectionName) {
    this.collectionName = collectionName;
    this.noSqlRecords = noSqlRecords;
  }

  @Override
  public void createIndex(String field, String index) {
    noSqlRecords.createIndex(collectionName, field, index);
  }

  @Override
  public JSONObject lookupEntireRecord(String name) throws RecordNotFoundException, FailedDBOperationException {
    return noSqlRecords.lookupEntireRecord(collectionName, name);
  }

  @Override
  public HashMap<ColumnField, Object> lookupUserFields(String name, ColumnField nameField,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException {
    return noSqlRecords.lookupSomeFields(collectionName, name, nameField, valuesMapField, valuesMapKeys);
  }

  @Override
  public void addRecord(JSONObject json) throws FailedDBOperationException, RecordExistsException {
    NoSQLRecords records = noSqlRecords;
    try {
      String name = json.getString(NameRecord.NAME.getName());
      records.insert(collectionName, name, json);
      GNSConfig.getLogger().log(Level.FINER, "{0}:: Added {1} JSON: {2}",
              new Object[]{records.toString(), name, json});
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "{0}:: Error adding name record: {1}",
              new Object[]{records.toString(), e});
    }
  }

  @Override
  public void removeRecord(String name) throws FailedDBOperationException {
    noSqlRecords.removeEntireRecord(collectionName, name);
  }

  @Override
  public boolean containsName(String name) throws FailedDBOperationException {
    return noSqlRecords.contains(collectionName, name);
  }

  @Override
  public void updateEntireValuesMap(String name, ValuesMap valuesMap)
          throws FailedDBOperationException {
    noSqlRecords.updateEntireRecord(collectionName, name, valuesMap);
  }

  @Override
  public void updateIndividualFields(String name, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException {
    noSqlRecords.updateIndividualFields(collectionName, name,
            NameRecord.VALUES_MAP, valuesMapKeys, valuesMapValues);
  }

  @Override
  public void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException {
    noSqlRecords.removeMapKeys(collectionName, name, mapField, mapKeys);
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator() throws FailedDBOperationException {
    return noSqlRecords.getAllRowsIterator(collectionName);
  }

  @Override
  public AbstractRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException {
    return noSqlRecords.selectRecords(collectionName, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) throws FailedDBOperationException {
    return noSqlRecords.selectRecordsWithin(collectionName, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException {
    return noSqlRecords.selectRecordsNear(collectionName, valuesMapField, key, value, maxDistance);
  }

  @Override
  public AbstractRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) throws FailedDBOperationException {
    return noSqlRecords.selectRecordsQuery(collectionName, valuesMapField, query);
  }

  @Override
  public String toString() {
    return "MongoRecordMap{" + "collectionName=" + collectionName + ", records=" + noSqlRecords + '}';
  }

}
