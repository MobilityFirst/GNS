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

import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsapp.recordmap.GNSRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Basic test for the GNS using the UniversalTcpClient.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NoSQLTest {

  private static String node = "testNode";
  private static String collection = "testCollection";
  private static String guid = "testGuid";
  private static String field = "testField";
  private static DiskMapRecords instance;
  private static GNSRecordMap<String> recordMap;

  public NoSQLTest() {
    if (instance == null) {
      instance = new DiskMapRecords(node);
      recordMap = new GNSRecordMap<>(instance, collection);
    }
  }

  @Test
  public void test_01_Insert() {
    JSONObject json = new JSONObject();
    try {
      json.put(field, "some value");
    } catch (JSONException e) {
      fail("Problem creating json " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    NameRecord nameRecord = new NameRecord(recordMap, guid, valuesMap);
    try {
      instance.insert(collection, guid, nameRecord.toJSONObject());
    } catch (FailedDBOperationException | JSONException | RecordExistsException e) {
      fail("Problem during insert " + e);
    }
  }

  @Test
  public void test_02_PrintAll() {
    try {
      System.out.println("All entries:");
      instance.printAllEntries(collection);
    } catch (FailedDBOperationException e) {
      fail("Problem showing all entries: " + e);
    }
  }

  @Test
  public void test_03_LookupEntireRecord() {
    JSONObject json = new JSONObject();
    try {
      json.put(field, "some value");
    } catch (JSONException e) {
      fail("Problem creating json: " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    JSONObject expected = new JSONObject();
    try {
      expected.put(NameRecord.NAME.getName(), guid);
      expected.put(NameRecord.VALUES_MAP.getName(), valuesMap);
    } catch (JSONException e) {
      fail("Problem creating json expected value: " + e);
    }

    try {
      JSONAssert.assertEquals(
              expected,
              instance.lookupEntireRecord(collection, guid),
              JSONCompareMode.STRICT);
    } catch (RecordNotFoundException | FailedDBOperationException | JSONException e) {
      fail("Problem during LookupEntireRecord: " + e);
    }
  }

  @Test
  public void test_04_LookupSomeFields() {
    Map<ColumnField, Object> expected = new HashMap<>();
    expected.put(NameRecord.NAME, guid);
    JSONObject json = new JSONObject();
    try {
      json.put(field, "some value");
    } catch (JSONException e) {
      fail("Problem creating json: " + e);
    }
    expected.put(NameRecord.VALUES_MAP, new ValuesMap(json));

    try {
      Map<ColumnField, Object> actual = instance.lookupSomeFields(
              collection, guid,
              NameRecord.NAME,
              NameRecord.VALUES_MAP,
              new ArrayList<>(Arrays.asList(new ColumnField(field,
                      ColumnFieldType.USER_JSON))));

      assertEquals(expected.get(NameRecord.NAME),
              actual.get(NameRecord.NAME));
      JSONAssert.assertEquals(
              ((JSONObject) expected.get(NameRecord.VALUES_MAP)),
              ((JSONObject) actual.get(NameRecord.VALUES_MAP)),
              JSONCompareMode.STRICT);

    } catch (RecordNotFoundException | FailedDBOperationException | JSONException e) {
      fail("Problem during LookupSomeFields: " + e);
    }
  }

  @Test
  public void test_05_RemoveEntireRecord() {
    try {
      instance.removeEntireRecord(collection, guid);
    } catch (FailedDBOperationException e) {
      fail("Problem while deleting record: " + e);
    }
  }

  @Test
  public void test_06_CheckForRecordGone() {
    try {
      JSONObject json = instance.lookupEntireRecord(collection, guid);
      fail("Record should not exist: " + json);
    } catch (RecordNotFoundException | FailedDBOperationException e) {

    }
  }

  private static String guid2 = "guid#2";

  @Test
  public void test_20_InsertRecord() {
    JSONObject json = new JSONObject();
    try {
      json.put(field, "some value");
    } catch (JSONException e) {
      fail("Problem creating json " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    NameRecord nameRecord = new NameRecord(recordMap, guid2, valuesMap);
    try {
      instance.insert(collection, guid2, nameRecord.toJSONObject());
    } catch (FailedDBOperationException | JSONException | RecordExistsException e) {
      fail("Problem during insert " + e);
    }
  }

  @Test
  public void test_21_UpdateEntireRecord() {
    JSONObject json = new JSONObject();
    try {
      JSONObject innerJson = new JSONObject();
      innerJson.put("key", "value");
      json.put("map", innerJson);
    } catch (JSONException e) {
      fail("Problem creating json " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    NameRecord nameRecord = new NameRecord(recordMap, guid2, valuesMap);
    try {
      instance.updateEntireRecord(collection, guid2, nameRecord.getValuesMap());
    } catch (FailedDBOperationException | FieldNotFoundException e) {
      fail("Problem during insert " + e);
    }
  }

  @Test
  public void test_22_LookupEntireRecordGuid2() {
    JSONObject json = new JSONObject();
    try {
      JSONObject innerJson = new JSONObject();
      innerJson.put("key", "value");
      json.put("map", innerJson);
    } catch (JSONException e) {
      fail("Problem creating json " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    JSONObject expected = new JSONObject();
    try {
      expected.put(NameRecord.NAME.getName(), guid2);
      expected.put(NameRecord.VALUES_MAP.getName(), valuesMap);
    } catch (JSONException e) {
      fail("Problem creating json expected value: " + e);
    }

    try {
      JSONAssert.assertEquals(
              expected,
              instance.lookupEntireRecord(collection, guid2),
              JSONCompareMode.STRICT);
    } catch (RecordNotFoundException | FailedDBOperationException | JSONException e) {
      fail("Problem during LookupEntireRecord: " + e);
    }
  }

  @Test
  public void test_23_UpdateIndividualFields() {
    try {
      instance.updateIndividualFields(collection, guid2,
              NameRecord.VALUES_MAP,
              new ArrayList<>(Arrays.asList(new ColumnField("new", ColumnFieldType.USER_JSON))),
              new ArrayList<>(Arrays.asList("newValue"))
      );
    } catch (FailedDBOperationException e) {
      fail("Problem during insert " + e);
    }
  }

  @Test
  public void test_24_LookupEntireRecordGuid2Again() {
    JSONObject json = new JSONObject();
    try {
      JSONObject innerJson = new JSONObject();
      innerJson.put("key", "value");
      json.put("map", innerJson);
      json.put("new", "newValue");
    } catch (JSONException e) {
      fail("Problem creating json " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    JSONObject expected = new JSONObject();
    try {
      expected.put(NameRecord.NAME.getName(), guid2);
      expected.put(NameRecord.VALUES_MAP.getName(), valuesMap);
    } catch (JSONException e) {
      fail("Problem creating json expected value: " + e);
    }

    try {
      JSONAssert.assertEquals(
              expected,
              instance.lookupEntireRecord(collection, guid2),
              JSONCompareMode.STRICT);
    } catch (RecordNotFoundException | FailedDBOperationException | JSONException e) {
      fail("Problem during LookupEntireRecord: " + e);
    }
  }

  @Test
  public void test_25_LookupSomeDottedFields() {
    try {
      Map<ColumnField, Object> actual = instance.lookupSomeFields(
              collection, guid2,
              NameRecord.NAME,
              NameRecord.VALUES_MAP,
              new ArrayList<>(Arrays.asList(new ColumnField("map.key",
                      ColumnFieldType.USER_JSON))));

      
      assertEquals("value", 
              ((JSONObject)actual.get(NameRecord.VALUES_MAP)).get("map.key"));

    } catch (RecordNotFoundException | FailedDBOperationException | JSONException e) {
      fail("Problem during LookupSomeFields: " + e);
    }
  }

  @Test
  public void test_30_RemoveMapKeys() {
    try {
      instance.removeMapKeys(collection, "guid#2",
              NameRecord.VALUES_MAP,
              new ArrayList<>(Arrays.asList(
                      new ColumnField("new", ColumnFieldType.USER_JSON),
                      new ColumnField("map", ColumnFieldType.USER_JSON))));
    } catch (FailedDBOperationException e) {
      fail("Problem while deleting record: " + e);
    }
  }

  @Test
  public void test_31_LookupEntireRecordGuid2YetAgain() {
    JSONObject json = new JSONObject();
    ValuesMap valuesMap = new ValuesMap(json);
    JSONObject expected = new JSONObject();
    try {
      expected.put(NameRecord.NAME.getName(), guid2);
      expected.put(NameRecord.VALUES_MAP.getName(), valuesMap);
    } catch (JSONException e) {
      fail("Problem creating json expected value: " + e);
    }

    try {
      JSONAssert.assertEquals(
              expected,
              instance.lookupEntireRecord(collection, guid2),
              JSONCompareMode.STRICT);
    } catch (RecordNotFoundException | FailedDBOperationException | JSONException e) {
      fail("Problem during LookupEntireRecord: " + e);
    }
  }
}
