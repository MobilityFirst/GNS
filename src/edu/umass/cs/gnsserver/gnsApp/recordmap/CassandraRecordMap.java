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
import edu.umass.cs.gnsserver.database.CassandraRecords;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Supports Cassandra record. Not fully implemented.
 * 
 */
public class CassandraRecordMap extends BasicRecordMap {

  private String collectionName;
  
  private CassandraRecords cassandraRecords;
  
  /**
   * Create a CassandraRecordMap instance by copying.
   * 
   * @param cassandraRecords
   * @param collectionName
   */
  public CassandraRecordMap(CassandraRecords cassandraRecords, String collectionName) {
    this.collectionName = collectionName;
    this.cassandraRecords = cassandraRecords;
  }

  /**
   * Create a empty CassandraRecordMap instance.
   * 
   * @param collectionName
   */
  public CassandraRecordMap(String collectionName) {
    this.collectionName = collectionName;
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
    if (!containsName(name)) {
      try {
        CassandraRecords records = this.cassandraRecords;
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
  public HashMap<ColumnField, Object> lookupMultipleSystemFields(String name, ColumnField nameField, ArrayList<ColumnField> fields1) throws RecordNotFoundException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public HashMap<ColumnField, Object> lookupMultipleSystemAndUserFields(String name, ColumnField nameField, ArrayList<ColumnField> fields1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor getAllRowsIterator() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NameRecord getNameRecord(String name) {
    try {
      JSONObject json = this.cassandraRecords.lookupEntireRecord(collectionName, name);
      if (json == null) {
        return null;
      } else {
        return new NameRecord(this, json);
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting name record " + name + ": " + e);
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void addNameRecord(NameRecord recordEntry) {

    try {
      addNameRecord(recordEntry.toJSONObject());
      //this.cassandraRecords.insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Error adding name record: " + e);
      return;
    }
  }

  /**
   *
   * @param json
   */
  @Override
  public void addNameRecord(JSONObject json) {
    CassandraRecords records = this.cassandraRecords;
    try {
      String name = json.getString(NameRecord.NAME.getName());
      records.insert(collectionName, name, json);
      GNS.getLogger().finer(records.toString() + ":: Added " + name);
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws FailedDBOperationException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    try {
      this.cassandraRecords.update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field found found exception: " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void removeNameRecord(String name) {
    this.cassandraRecords.removeEntireRecord(collectionName, name);
  }

  @Override
  public boolean containsName(String name) {
    return this.cassandraRecords.contains(collectionName, name);
  }

  @Override
  public void reset() {
    this.cassandraRecords.reset(collectionName);
  }

  @Override
  public AbstractRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public AbstractRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
