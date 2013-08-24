package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.CassandraRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

public class CassandraRecordMap extends BasicRecordMap {

  private String collectionName;

  public CassandraRecordMap(String collectionName) {
    this.collectionName = collectionName;
  }

  @Override
  public String getNameRecordField(String name, String key) {
    CassandraRecords records = CassandraRecords.getInstance();
    String result = records.lookup(collectionName, name, key);
    if (result != null) {
      GNS.getLogger().fine(records.toString() + ":: Retrieved " + name + "/" + key + ": " + result);
      return result;
    } else {
      GNS.getLogger().fine(records.toString() + ":: No record named " + name + " with key " + key);
      return null;
    }
  }

  @Override
  public ArrayList<String> getNameRecordFields(String name, ArrayList<String> keys) {
    CassandraRecords records = CassandraRecords.getInstance();
    ArrayList<String> result = records.lookup(collectionName, name, keys);
    if (result != null) {
      GNS.getLogger().fine(records.toString() + ":: Retrieved " + name + "/" + keys + ": " + result);
      return result;
    } else {
      GNS.getLogger().fine(records.toString() + ":: No record named " + name + " with key " + result);
      return null;
    }
  }

  @Override
  public void updateNameRecordListValue(String name, String key, ArrayList<String> value) {
    CassandraRecords records = CassandraRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing list " + name + "/" + key + ": " + value.toString());
    records.updateField(collectionName, name, key, value);
  }

  @Override
  public void updateNameRecordListValueInt(String name, String key, Set<Integer> value) {
    CassandraRecords records = CassandraRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing int list " + name + "/" + key + ": " + value.toString());
    records.updateField(collectionName, name, key, value);
  }

  @Override
  public void updateNameRecordFieldAsString(String name, String key, String string) {
    CassandraRecords records = CassandraRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing string " + name + "/" + key + ": " + string);
    records.updateField(collectionName, name, key, string);
  }
  
  @Override
  public void updateNameRecordFieldAsMap(String name, String key, Map map) {
    CassandraRecords records = CassandraRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing map " + name + "/" + key + ": " + map);
    records.updateField(collectionName, name, key, map);
  }
  
  @Override
  public void updateNameRecordFieldAsCollection(String name, String key, Collection collection) {
    CassandraRecords records = CassandraRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing collection " + name + "/" + key + ": " + collection);
    records.updateField(collectionName, name, key, collection);
  }

  @Override
  public Set<String> getAllRowKeys() {
    CassandraRecords records = CassandraRecords.getInstance();
    return records.keySet(collectionName);
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
    if (!containsName(name)) {
      try {
        CassandraRecords records = CassandraRecords.getInstance();
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
  public NameRecord getNameRecordLazy(String name) {
    if (CassandraRecords.getInstance().contains(collectionName, name)) {
      //GNS.getLogger().info("Creating lazy name record for " + name);
      return new NameRecord(name, this);
    } else {
      return null;
    }
  }

  @Override
  public NameRecord getNameRecordLazy(String name, ArrayList<String> keys) {
    ArrayList<String> values = CassandraRecords.getInstance().lookup(collectionName,name,keys);
    if (values == null) return null;
    return new NameRecord(name, this, keys,values);

  }

  @Override
  public NameRecord getNameRecord(String name) {
    try {
      JSONObject json = CassandraRecords.getInstance().lookup(collectionName, name);
      if (json == null) {
        return null;
      } else {
        return new NameRecord(json);
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Error getting name record " + name + ": " + e);
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void addNameRecord(NameRecord recordEntry) {
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Start addNameRecord " + recordEntry.getName());
    }
    try {
      addNameRecord(recordEntry.toJSONObject());
      //CassandraRecords.getInstance().insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Error adding name record: " + e);
      return;
    }
  }

  @Override
  public void addNameRecord(JSONObject json) {
    CassandraRecords records = CassandraRecords.getInstance();
    try {
      String name = json.getString(NameRecord.NAME);
      records.insert(collectionName, name, json);
      GNS.getLogger().finer(records.toString() + ":: Added " + name);
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    try {
      CassandraRecords.getInstance().update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void removeNameRecord(String name) {
    CassandraRecords.getInstance().remove(collectionName, name);
  }

  @Override
  public boolean containsName(String name) {
    return CassandraRecords.getInstance().contains(collectionName, name);
  }

  @Override
  public Set<NameRecord> getAllNameRecords() {
    //CassandraRecords.getInstance().keySet(collectionName);
    CassandraRecords records = CassandraRecords.getInstance();
    Set<NameRecord> result = new HashSet<NameRecord>();
    for (JSONObject json : records.retrieveAllEntries(collectionName)) {
      try {
        result.add(new NameRecord(json));
      } catch (JSONException e) {
        GNS.getLogger().severe(records.toString() + ":: Error getting name record: " + e);
        e.printStackTrace();
      }
    }
    return result;
  }

  @Override
  public void reset() {
    CassandraRecords.getInstance().reset(collectionName);
  }

  @Override
  public ReplicaControllerRecord getNameRecordPrimary(String name) {
    try {
      JSONObject json = CassandraRecords.getInstance().lookup(collectionName, name);
      if (json == null) {
        return null;
      } else {
        return new ReplicaControllerRecord(json);
      }
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }
  
  @Override
  public ReplicaControllerRecord getNameRecordPrimaryLazy(String name) {
    if (CassandraRecords.getInstance().contains(collectionName, name)) {
      //GNS.getLogger().info("Creating lazy name record for " + name);
      return new ReplicaControllerRecord(name, this);
    } else {
      return null;
    }
  }

  @Override
  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    if (StartNameServer.debugMode) {
      GNS.getLogger().fine("Start addNameRecord " + recordEntry.getName());
    }

    try {
      CassandraRecords.getInstance().insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }
  }

  @Override
  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    try {
      CassandraRecords.getInstance().update(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public Set<ReplicaControllerRecord> getAllPrimaryNameRecords() {
    //CassandraRecords.getInstance().keySet(collectionName);
    CassandraRecords records = CassandraRecords.getInstance();
    Set<ReplicaControllerRecord> result = new HashSet<ReplicaControllerRecord>();
    for (JSONObject json : records.retrieveAllEntries(collectionName)) {
      try {
        result.add(new ReplicaControllerRecord(json));
      } catch (JSONException e) {
        GNS.getLogger().severe(records.toString() + ":: Error getting name record: " + e);
        e.printStackTrace();
      }
    }
    return result;
//        return MongoRecordMap.g;
  }

  // test code
  public static void main(String[] args) throws Exception {
    NameServer.nodeID = 4;
    retrieveFieldTest();
    //System.exit(0);
  }

  private static void retrieveFieldTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    BasicRecordMap recordMap = new CassandraRecordMap(CassandraRecords.DBNAMERECORD);
    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", NameRecord.PRIMARY_NAMESERVERS));
    recordMap.updateNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED", new HashSet<Integer>(Arrays.asList(1, 2, 3)));
    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED"));
  }
}
