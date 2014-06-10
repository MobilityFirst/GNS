package edu.umass.cs.gns.nsdesign.recordmap;

import edu.umass.cs.gns.database.AbstractRecordCursor;
import edu.umass.cs.gns.database.CassandraRecords;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
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
public class CassandraRecordMap extends BasicRecordMap {

  private String collectionName;
  
  private CassandraRecords cassandraRecords;
  
  public CassandraRecordMap(CassandraRecords cassandraRecords, String collectionName) {
    this.collectionName = collectionName;
    this.cassandraRecords = cassandraRecords;
  }

  public CassandraRecordMap(String collectionName) {
    this.collectionName = collectionName;
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
    if (!containsName(name)) {
      try {
        CassandraRecords records = this.cassandraRecords;
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
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1,
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
      JSONObject json = this.cassandraRecords.lookup(collectionName, name);
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
  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws FailedUpdateException {

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
    this.cassandraRecords.remove(collectionName, name);
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
  public ReplicaControllerRecord getNameRecordPrimary(String name) {
    try {
      JSONObject json = this.cassandraRecords.lookup(collectionName, name);
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
  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    try {
      this.cassandraRecords.insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    try {
      this.cassandraRecords.update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (FieldNotFoundException e) {
      GNS.getLogger().severe("Field not found " + e.getMessage());
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
//  // test code
//  public static void main(String[] args) throws Exception {
//    NameServer.nodeID = 4;
//    retrieveFieldTest();
//    //System.exit(0);
//  }
//
//  private static void retrieveFieldTest() throws Exception {
//    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
//    ConsistentHashing.initializeHashFunction();
//    BasicRecordMap recordMap = new CassandraRecordMap(CassandraRecords.DBNAMERECORD);
//    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", NameRecord.PRIMARY_NAMESERVERS.getName()));
//    recordMap.updateNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED", new HashSet<Integer>(Arrays.asList(1, 2, 3)));
//    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED"));
//  }

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
