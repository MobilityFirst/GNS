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

import static edu.umass.cs.gnsserver.database.MongoRecords.DBNAMERECORD;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.RecordExistsException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.recordmap.MongoRecordMap;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Ascertains the maximum throughput of reads on the mongo database.
 *
 * Typical incantation:
 *
 * java -cp GNS.jar edu.umass.cs.gnsserver.test.MongoRecordsThroughputTest frank_ActiveReplica CAB372BF40B3DB576786E5CC6AB05B63CC680F4D environment
 *
 * @author westy
 */
public class MongoRecordsThroughputTest {

  /**
   * Run the test.
   * 
   * @param args
   * @throws Exception
   * @throws RecordNotFoundException 
   */
  public static void main(String[] args) throws Exception, RecordNotFoundException {
    if (args.length == 3) {
      testlookupMultipleSystemAndUserFields(args[0], args[1], args[2]);
    } else {
      System.out.println("Usage: edu.umass.cs.gnsserver.test.MongoRecordsThroughputTest <node> <guid> <field>");
    }
    // important to include this!!
    System.exit(0);
  }

  private static final ArrayList<ColumnField> dnsSystemFields = new ArrayList<ColumnField>();

  static {
    dnsSystemFields.add(NameRecord.ACTIVE_VERSION);
    dnsSystemFields.add(NameRecord.TIME_TO_LIVE);
  }

  private static void testlookupMultipleSystemAndUserFields(String node, String guid, String field) {

    // make a fake record
    MongoRecords<String> instance = new MongoRecords<String>(node);
    MongoRecordMap<String> recordMap = new MongoRecordMap<String>(instance, DBNAMERECORD);
    JSONObject json = new JSONObject();
    try {
      json.put(field, "some value");
    } catch (JSONException e) {
      System.out.println("Problem creating json " + e);
    }
    ValuesMap valuesMap = new ValuesMap(json);
    NameRecord nameRecord = new NameRecord(recordMap, guid, 0, valuesMap, 0, new HashSet<String>());
    try {
      instance.insert(DBNAMERECORD, guid, nameRecord.toJSONObject());
    } catch (JSONException e) {
      System.out.println("Problem writing json " + e);
    } catch (FailedDBOperationException e) {
      System.out.println("Problem adding " + json.toString() + " as value of " + guid + ": " + e);
    } catch (RecordExistsException e) {
      System.out.println(guid + " record already exists in database. Try something else." + e);
    }

    // and try to read it as fast as possible
    try {
      ArrayList<ColumnField> userFields
              = new ArrayList<ColumnField>(Arrays.asList(new ColumnField(field,
                                      ColumnFieldType.USER_JSON)));
      int cnt = 0;
      long startTime = System.currentTimeMillis();
      do {
        Map<ColumnField, Object> map
                = instance.lookupMultipleSystemAndUserFields(
                        DBNAMERECORD,
                        guid,
                        NameRecord.NAME,
                        dnsSystemFields,
                        NameRecord.VALUES_MAP,
                        userFields);
        if (cnt++ % 10000 == 0) {
          System.out.println(map);
          System.out.println(DelayProfiler.getStats());
          System.out.println("op/s = " + Format.formatTime(10000000.0 / (System.currentTimeMillis() - startTime)));
          startTime = System.currentTimeMillis();
        }
      } while (true);
    } catch (FailedDBOperationException | RecordNotFoundException e) {
      System.out.println("Lookup failed: " + e);
    }
  }
}
