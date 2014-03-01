package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class MongoRecordMap extends BasicRecordMap {

  private String collectionName;

  public MongoRecordMap(String collectionName) {
    this.collectionName = collectionName;
  }

  @Override
  public Set<String> getAllRowKeys() {
    MongoRecords records = MongoRecords.getInstance();
    return records.keySet(collectionName);
  }

  @Override
  public Set<String> getAllColumnKeys(String name) throws RecordNotFoundException {
    if (!containsName(name)) {
      try {
        MongoRecords records = MongoRecords.getInstance();
        JSONObject json = records.lookup(collectionName, name);
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
  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1) throws RecordNotFoundException {
    return MongoRecords.getInstance().lookup(collectionName, name, nameField, fields1);
//    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException {
    return MongoRecords.getInstance().lookup(collectionName, name, nameField, fields1, valuesMapField, valuesMapKeys);
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    MongoRecords.getInstance().update(collectionName, name, nameField, fields1, values1);
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    MongoRecords.getInstance().update(collectionName, name, nameField, fields1, values1, valuesMapField, valuesMapKeys, valuesMapValues);
  }

  @Override
  public void updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue,
                                ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField,
                                ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    MongoRecords.getInstance().updateConditional(collectionName, name, nameField, conditionField, conditionValue,
            fields1, values1, valuesMapField, valuesMapKeys, valuesMapValues);
  }

  @Override
  public void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    MongoRecords.getInstance().increment(collectionName, name, fields1, values1);
  }

  @Override
  public void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues) {
    MongoRecords.getInstance().increment(collectionName, name, fields1, values1, votesMapField, votesMapKeys, votesMapValues);
  }

  @Override
  public void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys) {
    MongoRecords.getInstance().removeMapKeys(collectionName, name, mapField, mapKeys);
  }

  @Override
  public BasicRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields) {
    return MongoRecords.getInstance().getAllRowsIterator(collectionName, nameField, fields);
  }

  @Override
  public BasicRecordCursor getAllRowsIterator() {
    return MongoRecords.getInstance().getAllRowsIterator(collectionName);
  }

  @Override
  public BasicRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) {
    return MongoRecords.getInstance().selectRecords(collectionName, valuesMapField, key, value);
  }
  
  @Override
  public BasicRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) {
     return MongoRecords.getInstance().selectRecordsWithin(collectionName, valuesMapField, key, value);
  }

  @Override
  public BasicRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) {
    return MongoRecords.getInstance().selectRecordsNear(collectionName, valuesMapField, key, value, maxDistance);
  }
  
   @Override
  public BasicRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) {
    return MongoRecords.getInstance().selectRecordsQuery(collectionName, valuesMapField, query);
  }
  
  @Override
  public NameRecord getNameRecord(String name) throws RecordNotFoundException {
    try {
      JSONObject json = MongoRecords.getInstance().lookup(collectionName, name);
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
  public void addNameRecord(NameRecord recordEntry) throws RecordExistsException {
    if (StartNameServer.debugMode) {
      try {
        GNS.getLogger().fine("Start addNameRecord " + recordEntry.getName());
      } catch (FieldNotFoundException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        return;
      }
    }
//    GNS.getLogger().fine("here ..");
    try {
      addNameRecord(recordEntry.toJSONObject());
      //MongoRecords.getInstance().insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Error adding name record: " + e);
      return;
    }
//    GNS.getLogger().fine("here 2 ..");
  }

  @Override
  public void addNameRecord(JSONObject json) throws RecordExistsException {
    MongoRecords records = MongoRecords.getInstance();
    try {
      String name = json.getString(NameRecord.NAME.getName());
      records.insert(collectionName, name, json);
      GNS.getLogger().finer(records.toString() + ":: Added " + name + " JSON: " + json);
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws RecordExistsException {
    MongoRecords records = MongoRecords.getInstance();
    records.bulkInsert(collectionName, jsons);
    GNS.getLogger().finer(records.toString() + ":: Added all json records. JSON: " + jsons);
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    try {
      MongoRecords.getInstance().update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (FieldNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void removeNameRecord(String name) {
    MongoRecords.getInstance().remove(collectionName, name);
  }

  @Override
  public boolean containsName(String name) {
    return MongoRecords.getInstance().contains(collectionName, name);
  }

  @Override
  public void reset() {
    MongoRecords.getInstance().reset(collectionName);
  }

  @Override
  public ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException {
    try {
      JSONObject json = MongoRecords.getInstance().lookup(collectionName, name);
//      GNS.getLogger().severe("JSON primary is " + json);
      if (json == null) {
        return null;
      } else {
        return new ReplicaControllerRecord(this, json);
      }
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }
  
  @Override
  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) throws RecordExistsException {
    try {
      MongoRecords.getInstance().insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found: " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    try {
      MongoRecords.getInstance().update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found: " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
//
//  // test code
//  public static void main(String[] args) throws Exception {
//    NameServer.nodeID = 2;
//    retrieveFieldTest();
//    //System.exit(0);
//  }
//
//  private static void retrieveFieldTest() throws Exception {
//    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
//    ConsistentHashing.initializeHashFunction();
//    BasicRecordMap recordMap = new MongoRecordMap(MongoRecords.DBNAMERECORD);
//    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", NameRecord.PRIMARY_NAMESERVERS.getName()));
//    recordMap.updateNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED", new HashSet<Integer>(Arrays.asList(1, 2, 3)));
//    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED"));
//  }

  
}
