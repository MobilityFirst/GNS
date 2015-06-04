/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.database;

import static edu.umass.cs.gns.database.MongoRecords.DBNAMERECORD;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.newApp.recordmap.MongoRecordMap;
import edu.umass.cs.gns.newApp.recordmap.NameRecord;
import edu.umass.cs.gns.util.Format;
import edu.umass.cs.gns.util.ValuesMap;
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
 * java -cp GNS.jar edu.umass.cs.gns.test.MongoRecordsThroughputTest frank_ActiveReplica CAB372BF40B3DB576786E5CC6AB05B63CC680F4D environment
 *
 * @author westy
 */
public class MongoRecordsThroughputTest {

  public static void main(String[] args) throws Exception, RecordNotFoundException {
    if (args.length == 3) {
      testlookupMultipleSystemAndUserFields(args[0], args[1], args[2]);
    } else {
      System.out.println("Usage: edu.umass.cs.gns.test.MongoRecordsThroughputTest <node> <guid> <field>");
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
    MongoRecords instance = new MongoRecords(node);
    MongoRecordMap recordMap = new MongoRecordMap(instance, DBNAMERECORD);
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
