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

import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordExistsException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Supports abstract access to a collection of MondoDB records specified by the
 * <code>collectionName</code> string.
 * 
 * @author westy
 * @param <NodeIDType> 
 */
public class MongoRecordMap<NodeIDType> extends BasicRecordMap {

  private final String collectionName;
  private final MongoRecords<NodeIDType> mongoRecords;

  /**
   * Creates an MongoRecordMap instance.
   * 
   * @param mongoRecords
   * @param collectionName
   */
  public MongoRecordMap(MongoRecords<NodeIDType> mongoRecords, String collectionName) {
    this.collectionName = collectionName;
    this.mongoRecords = mongoRecords;

  }

  @Override
  public Set<String> getAllColumnKeys(String name) throws RecordNotFoundException, FailedDBOperationException {
    if (!containsName(name)) {
      try {
        MongoRecords<NodeIDType> records = mongoRecords;
        JSONObject json = records.lookupEntireRecord(collectionName, name);
        return JSONUtils.JSONArrayToSetString(json.names());
      } catch (JSONException e) {
        GNS.getLogger().severe("Error updating json record: " + e);
        return null;
      }
    } else {
      return null;
    }
  }


  @Override
  public HashMap<ColumnField, Object> lookupMultipleSystemFields(String name, ColumnField nameField, ArrayList<ColumnField> systemFields) throws RecordNotFoundException, FailedDBOperationException {
    return mongoRecords.lookupMultipleSystemFields(collectionName, name, nameField, systemFields);
  }

  @Override
  public HashMap<ColumnField, Object> lookupMultipleSystemAndUserFields(String name, ColumnField nameField, ArrayList<ColumnField> systemFields,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException, FailedDBOperationException {
    return mongoRecords.lookupMultipleSystemAndUserFields(collectionName, name, nameField, systemFields, valuesMapField, valuesMapKeys);
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> systemFields, ArrayList<Object> systemValues)
          throws FailedDBOperationException {
    mongoRecords.update(collectionName, name, nameField, systemFields, systemValues);
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> systemFields, ArrayList<Object> systemValues,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException {
    mongoRecords.update(collectionName, name, nameField, systemFields, systemValues, valuesMapField, valuesMapKeys, valuesMapValues);
  }

  @Override
  public boolean updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue,
          ArrayList<ColumnField> systemFields, ArrayList<Object> systemValues, ColumnField valuesMapField,
          ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException {
    return mongoRecords.updateConditional(collectionName, name, nameField, conditionField, conditionValue,
            systemFields, systemValues, valuesMapField, valuesMapKeys, valuesMapValues);
  }

  @Override
  public void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException {
    mongoRecords.removeMapKeys(collectionName, name, mapField, mapKeys);
  }

  @Override
  public AbstractRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields) throws FailedDBOperationException {
    return mongoRecords.getAllRowsIterator(collectionName, nameField, fields);
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator() throws FailedDBOperationException {
    return mongoRecords.getAllRowsIterator(collectionName);
  }

  @Override
  public AbstractRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException {
    return mongoRecords.selectRecords(collectionName, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) throws FailedDBOperationException {
    return mongoRecords.selectRecordsWithin(collectionName, valuesMapField, key, value);
  }

  @Override
  public AbstractRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException {
    return mongoRecords.selectRecordsNear(collectionName, valuesMapField, key, value, maxDistance);
  }

  @Override
  public AbstractRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) throws FailedDBOperationException {
    return mongoRecords.selectRecordsQuery(collectionName, valuesMapField, query);
  }

  @Override
  public NameRecord getNameRecord(String name) throws RecordNotFoundException, FailedDBOperationException {
    try {
      JSONObject json = mongoRecords.lookupEntireRecord(collectionName, name);
      if (json == null) {
        return null;
      } else {
        return new NameRecord(this, json);
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting name record " + name + ": " + e);
    }
    return null;
  }

  @Override
  public void addNameRecord(NameRecord recordEntry) throws FailedDBOperationException, RecordExistsException {
    try {
      addNameRecord(recordEntry.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Error adding name record: " + e);
      return;
    }
  }

  @Override
  public void addNameRecord(JSONObject json) throws FailedDBOperationException, RecordExistsException {
    MongoRecords records = mongoRecords;
    try {
      String name = json.getString(NameRecord.NAME.getName());
      records.insert(collectionName, name, json);
      GNS.getLogger().finer(records.toString() + ":: Added " + name + " JSON: " + json);
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error adding name record: " + e);
    }
  }

  @Override
  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws FailedDBOperationException, RecordExistsException {
    MongoRecords<NodeIDType> records = mongoRecords;
    records.bulkInsert(collectionName, jsons);
    GNS.getLogger().finer(records.toString() + ":: Added all json records. JSON: " + jsons);
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) throws FailedDBOperationException {
    try {
      mongoRecords.update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON Exception while converting record to JSON: " + e.getMessage());
    } catch (FieldNotFoundException e) {
    }
  }

  @Override
  public void removeNameRecord(String name) throws FailedDBOperationException {
    mongoRecords.removeEntireRecord(collectionName, name);
  }

  @Override
  public boolean containsName(String name) throws FailedDBOperationException {
    return mongoRecords.contains(collectionName, name);
  }

  @Override
  public void reset() throws FailedDBOperationException {
    mongoRecords.reset(collectionName);
  }

  @Override
  public String toString() {
    return "MongoRecordMap{" + "collectionName=" + collectionName + ", mongoRecords=" + mongoRecords + '}';
  }

}
