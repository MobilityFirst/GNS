package edu.umass.cs.gns.nsdesign.recordmap;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
//import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class MongoRecordMap extends BasicRecordMap {

  private String collectionName;
  private MongoRecords mongoRecords;
  public MongoRecordMap(MongoRecords mongoRecords, String collectionName) {
    this.collectionName = collectionName;
    this.mongoRecords = mongoRecords;

  }

  @Override
  public Set<String> getAllRowKeys() {
    MongoRecords records = mongoRecords;
    return records.keySet(collectionName);
  }

  @Override
  public Set<String> getAllColumnKeys(String name) throws RecordNotFoundException {
    if (!containsName(name)) {
      try {
        MongoRecords records = mongoRecords;
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
    return mongoRecords.lookup(collectionName, name, nameField, fields1);
//    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException {
    return mongoRecords.lookup(collectionName, name, nameField, fields1, valuesMapField, valuesMapKeys);
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    mongoRecords.update(collectionName, name, nameField, fields1, values1);
  }

  @Override
  public void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    mongoRecords.update(collectionName, name, nameField, fields1, values1, valuesMapField, valuesMapKeys, valuesMapValues);
  }

  @Override
  public void updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue,
                                ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField,
                                ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues) {
    mongoRecords.updateConditional(collectionName, name, nameField, conditionField, conditionValue,
            fields1, values1, valuesMapField, valuesMapKeys, valuesMapValues);
  }

  @Override
  public void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1) {
    mongoRecords.increment(collectionName, name, fields1, values1);
  }

  @Override
  public void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues) {
    mongoRecords.increment(collectionName, name, fields1, values1, votesMapField, votesMapKeys, votesMapValues);
  }

  @Override
  public void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys) {
    mongoRecords.removeMapKeys(collectionName, name, mapField, mapKeys);
  }

  @Override
  public BasicRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields) {
    return mongoRecords.getAllRowsIterator(collectionName, nameField, fields);
  }

  @Override
  public BasicRecordCursor getAllRowsIterator() {
    return mongoRecords.getAllRowsIterator(collectionName);
  }

  @Override
  public BasicRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) {
    return mongoRecords.selectRecords(collectionName, valuesMapField, key, value);
  }

  @Override
  public BasicRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) {
     return mongoRecords.selectRecordsWithin(collectionName, valuesMapField, key, value);
  }

  @Override
  public BasicRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) {
    return mongoRecords.selectRecordsNear(collectionName, valuesMapField, key, value, maxDistance);
  }

   @Override
  public BasicRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) {
    return mongoRecords.selectRecordsQuery(collectionName, valuesMapField, query);
  }

  @Override
  public NameRecord getNameRecord(String name) throws RecordNotFoundException {
    try {
      JSONObject json = mongoRecords.lookup(collectionName, name);
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
//    GNS.getLogger().fine("here ..");
    try {
      addNameRecord(recordEntry.toJSONObject());
      //mongoRecords.insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Error adding name record: " + e);
      return;
    }
//    GNS.getLogger().fine("here 2 ..");
  }

  @Override
  public void addNameRecord(JSONObject json) throws RecordExistsException {
    MongoRecords records = mongoRecords;
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
    MongoRecords records = mongoRecords;
    records.bulkInsert(collectionName, jsons);
    GNS.getLogger().finer(records.toString() + ":: Added all json records. JSON: " + jsons);
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    try {
      mongoRecords.update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (FieldNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void removeNameRecord(String name) {
    mongoRecords.remove(collectionName, name);
  }

  @Override
  public boolean containsName(String name) {
    return mongoRecords.contains(collectionName, name);
  }

  @Override
  /**
   * Clears the database and reinitializes all indices.
   */
  public void reset() {
    mongoRecords.reset(collectionName);
  }

  @Override
  public ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException {
    try {
      JSONObject json = mongoRecords.lookup(collectionName, name);
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
      mongoRecords.insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
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
      mongoRecords.update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
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
