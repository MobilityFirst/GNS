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
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Stores GUID, KEY, VALUE triples. Not fully implemented.
 *
 * @author westy
 */
public class InCoreRecordMapJSON extends BasicRecordMap {

  private static final String NAME = NameRecord.NAME.getName();

  private Map<String, JSONObject> recordMap;

  /**
   * Create an instance of InCoreRecordMapJSON.
   */
  public InCoreRecordMapJSON() {
    recordMap = new HashMap<String, JSONObject>();
  }

  @Override
  public void addNameRecord(JSONObject json) {
    try {
      recordMap.put(json.getString(NAME), json);
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting json record: " + e);
    }
  }

  @Override
  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws FailedDBOperationException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeNameRecord(String name) {
    recordMap.remove(name);
  }

  @Override
  public boolean containsName(String name) {
    return recordMap.containsKey(name);
  }

  @Override
  public void reset() {
    recordMap.clear();
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
    if (!containsName(name)) {
      try {
        return JSONUtils.JSONArrayToSetString(recordMap.get(name).names());
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
  public HashMap<ColumnField, Object> lookupMultipleSystemAndUserFields(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
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
    if (containsName(name)) {
      try {
        return new NameRecord(this, recordMap.get(name));
      } catch (JSONException e) {
        GNS.getLogger().severe("Error getting json record: " + e);
        return null;
      }
    } else {
      //System.out.println("&&&& NOT FOUND: " + name);
      return null;
    }
  }

  @Override
  public void addNameRecord(NameRecord recordEntry) {
    try {
      recordMap.put(recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting json record: " + e);
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found Exception: " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    addNameRecord(recordEntry);
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
