package edu.umass.cs.gns.newApp.recordmap;

import edu.umass.cs.gns.database.AbstractRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import edu.umass.cs.gns.util.JSONUtils;
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

  private String collectionName;
  private MongoRecords<NodeIDType> mongoRecords;

  
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
//    return null;  //To change body of implemented methods use File | Settings | File Templates.
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
  public void increment(String name, ArrayList<ColumnField> systemFields, ArrayList<Object> systemValues)
          throws FailedDBOperationException {
    mongoRecords.increment(collectionName, name, systemFields, systemValues);
  }

  @Override
  public void increment(String name, ArrayList<ColumnField> systemFields, ArrayList<Object> systemValues, ColumnField votesMapField,
          ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues)
          throws FailedDBOperationException {
    mongoRecords.increment(collectionName, name, systemFields, systemValues, votesMapField, votesMapKeys, votesMapValues);
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
//    GNS.getLogger().fine("here 2 ..");
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

  /**
   * Clears the database and reinitializes all indices.
   */
  @Override
  public void reset() throws FailedDBOperationException {
    mongoRecords.reset(collectionName);
  }

  @Override
  @Deprecated
  public ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException, FailedDBOperationException {
    try {
      JSONObject json = mongoRecords.lookupEntireRecord(collectionName, name);
      if (json == null) {
        return null;
      } else {
        return new ReplicaControllerRecord(this, json);
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON Exception while converting record to JSON: " + e.getMessage());
    }
    return null;
  }

  @Override
  @Deprecated
  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) throws FailedDBOperationException, RecordExistsException {
    try {
      mongoRecords.insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON Exception while converting record to JSON: " + e.getMessage());
      return;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found: " + e.getMessage());

    }
  }

  @Override
  @Deprecated
  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) throws FailedDBOperationException {
    try {
      mongoRecords.update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON Exception while converting record to JSON: " + e.getMessage());
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found: " + e.getMessage());
    }
  }
  
  @Override
  public String toString() {
    return "MongoRecordMap{" + "collectionName=" + collectionName + ", mongoRecords=" + mongoRecords + '}';
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
