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
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import static edu.umass.cs.gnsserver.database.MongoRecords.DBNAMERECORD;
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

  private String node = "testNode";
  private String guid = "testGuid";
  private String field = "testField";
  private DiskMapRecords instance;
  private GNSRecordMap<String> recordMap;

  public NoSQLTest() {
    if (instance == null) {
      instance = new DiskMapRecords(node);
      recordMap = new GNSRecordMap<String>(instance, DBNAMERECORD);
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
      instance.insert(DBNAMERECORD, guid, nameRecord.toJSONObject());
    } catch (FailedDBOperationException | JSONException | RecordExistsException e) {
      fail("Problem during insert " + e);
    }
  }

  @Test
  public void test_02_LookupEntireRecord() {
    JSONObject json = new JSONObject();
    try {
      json.put(field, "some value");
    } catch (JSONException e) {
      fail("Problem creating json " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    NameRecord nameRecord = new NameRecord(recordMap, guid, valuesMap);
    try {
      JSONAssert.assertEquals(
              nameRecord.toJSONObject(),
              instance.lookupEntireRecord(DBNAMERECORD, guid),
              JSONCompareMode.STRICT);
    } catch (RecordNotFoundException | FailedDBOperationException | JSONException e) {
      fail("Problem during LookupEntireRecord" + e);
    }
  }

  @Test
  public void test_03_LookupSomeFields() {
    Map<ColumnField, Object> expected = new HashMap<>();
    expected.put(NameRecord.NAME, guid);
    expected.put(new ColumnField(field, ColumnFieldType.USER_JSON), "some value");
    ArrayList<ColumnField> userFields = new ArrayList<>(Arrays.asList(new ColumnField(field,
            ColumnFieldType.USER_JSON)));

    try {
      assertEquals(expected,
              instance.lookupSomeFields(
                      DBNAMERECORD, guid, NameRecord.NAME, NameRecord.VALUES_MAP, userFields));
    } catch (RecordNotFoundException | FailedDBOperationException e) {
      fail("Problem during LookupEntireRecord" + e);
    }
  }
}
