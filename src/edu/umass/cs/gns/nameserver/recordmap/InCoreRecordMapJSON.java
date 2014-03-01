package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Stores GUID, KEY, VALUE triples
 *
 * @author westy
 */
public class InCoreRecordMapJSON extends BasicRecordMap {
  private static final String NAME = NameRecord.NAME.getName();

  private Map<String, JSONObject> recordMap;

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
  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws RecordExistsException {

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
  public Set<String> getAllRowKeys() {
    return recordMap.keySet();
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
  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1) throws RecordNotFoundException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException {
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
  public void updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BasicRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields) {
   throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BasicRecordCursor getAllRowsIterator() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BasicRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  //
  // THESE WILL BE DEPRECATED
  //
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

//  @Override
//  public Set<NameRecord> getAllNameRecords() {
//    Set<NameRecord> result = new HashSet();
//    for (Map.Entry<String, JSONObject> entry : recordMap.entrySet()) {
//      try {
//        result.add(new NameRecord(entry.getValue()));
//      } catch (JSONException e) {
//        GNS.getLogger().severe("Error getting json record: " + e);
//      }
//    }
//    return result;
//  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    addNameRecord(recordEntry);
  }
  
  @Override
  public ReplicaControllerRecord getNameRecordPrimary(String name) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  @Override
  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

//  @Override
//  public Set<ReplicaControllerRecord> getAllPrimaryNameRecords() {
//    throw new UnsupportedOperationException("Not supported yet.");
//  }

//  //
//  // TEST CODE
//  //
//  public static void main(String[] args) throws Exception, FieldNotFoundException {
//    test();
//  }
//
//  private static NameRecord createNameRecord(String name, String key, String value) throws Exception {
//    ValuesMap valuesMap = new ValuesMap();
//    valuesMap.put(key,new ArrayList(Arrays.asList(value)));
//    HashSet<Integer> x = new HashSet<Integer>();
//    x.add(0);
//    x.add(1);
//    x.add(2);
//    return new NameRecord(name, x, name+"-2",valuesMap);
//  }
//
//  private static void test() throws Exception, FieldNotFoundException {
//    ConfigFileInfo.readHostInfo("ns1", 4);
//    ConsistentHashing.initializeHashFunction();
//    InCoreRecordMapJSON recordMap = new InCoreRecordMapJSON();
//    NameRecord nameRecord = createNameRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRANK", "XYZ");
//    recordMap.addNameRecord(nameRecord);
//    nameRecord = recordMap.getNameRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
//    System.out.println(nameRecord);
//    if (nameRecord != null) {
//      System.out.println(nameRecord.getKey("_GNS_account_info"));
//      System.out.println(nameRecord.getKey("_GNS_guid_info"));
//    }
//    System.out.println(recordMap.getNameRecordField("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRANK"));
//    recordMap.updateNameRecordSingleValue("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRANK", "SLACKER");
//    System.out.println(recordMap.getNameRecordField("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRANK"));
//    System.out.println(recordMap.getAllRowKeys());
//    nameRecord = recordMap.getNameRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
//    System.out.println(nameRecord);
//    if (nameRecord != null) {
//      System.out.println(nameRecord.getKey("FRANK"));
//    }
//  }

  @Override
  public BasicRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BasicRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BasicRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
