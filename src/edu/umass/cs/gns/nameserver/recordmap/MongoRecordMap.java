package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.MongoRecords;
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
import java.util.HashSet;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;

public class MongoRecordMap extends BasicRecordMap {

  private String collectioName;

  public MongoRecordMap(String collectionName) {
    this.collectioName = collectionName;
  }

  @Override
  public String getNameRecordField(String name, String key) {
    MongoRecords records = MongoRecords.getInstance();
    String result = records.lookup(collectioName, name, key);
    if (result != null) {
      GNS.getLogger().fine(records.toString() + ":: Retrieved " + name + "/" + key + ": " + result);
      return result;
    } else {
      GNS.getLogger().fine(records.toString() + ":: No record named " + name + " with key " + key);
      return null;
    }
  }

  @Override
  public void updateNameRecordListValue(String name, String key, ArrayList<String> value) {
    MongoRecords records = MongoRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing list " + name + "/" + key + ": " + value.toString());
    records.updateListValue(collectioName, name, key, value);
  }

  @Override
  public void updateNameRecordField(String name, String key, String string) {
    MongoRecords records = MongoRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing string " + name + "/" + key + ": " + string);
    records.updateField(collectioName, name, key, string);
  }

  @Override
  public Set<String> getAllRowKeys() {
    MongoRecords records = MongoRecords.getInstance();
    return records.keySet(collectioName);
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
    if (!containsName(name)) {
      try {
        MongoRecords records = MongoRecords.getInstance();
        JSONObject json = records.lookup(collectioName, name);
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
    if (MongoRecords.getInstance().contains(collectioName, name)) {
      //GNS.getLogger().info("Creating lazy name record for " + name);
      return new NameRecord(name, this);
    } else {
      return null;
    }
  }

  @Override
  public NameRecord getNameRecord(String name) {
    try {
      JSONObject json = MongoRecords.getInstance().lookup(collectioName, name);
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
      //MongoRecords.getInstance().insert(collectionName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();
      GNS.getLogger().severe("Error adding name record: " + e);
      return;
    }
  }

  @Override
  public void addNameRecord(JSONObject json) {
    MongoRecords records = MongoRecords.getInstance();
    try {
      String name = json.getString(NameRecord.NAME);
      records.insert(collectioName, name, json);
      GNS.getLogger().finer(records.toString() + ":: Added " + name);
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    try {
      MongoRecords.getInstance().update(collectioName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void removeNameRecord(String name) {
    MongoRecords.getInstance().remove(collectioName, name);
  }

  @Override
  public boolean containsName(String name) {
    return MongoRecords.getInstance().contains(collectioName, name);
  }

  @Override
  public Set<NameRecord> getAllNameRecords() {
    MongoRecords.getInstance().keySet(collectioName);
    MongoRecords records = MongoRecords.getInstance();
    Set<NameRecord> result = new HashSet<NameRecord>();
    for (JSONObject json : records.retrieveAllEntries(collectioName)) {
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
    MongoRecords.getInstance().reset(collectioName);
  }

  @Override
  public ReplicaControllerRecord getNameRecordPrimary(String name) {
    try {
      JSONObject json = MongoRecords.getInstance().lookup(collectioName, name);
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
    if (MongoRecords.getInstance().contains(collectioName, name)) {
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
      MongoRecords.getInstance().insert(collectioName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      return;
    }
  }

  @Override
  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) {
    try {
      MongoRecords.getInstance().update(collectioName, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public Set<ReplicaControllerRecord> getAllPrimaryNameRecords() {
    MongoRecords.getInstance().keySet(collectioName);
    MongoRecords records = MongoRecords.getInstance();
    Set<ReplicaControllerRecord> result = new HashSet<ReplicaControllerRecord>();
    for (JSONObject json : records.retrieveAllEntries(collectioName)) {
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
    NameServer.nodeID = 2;
    retrieveFieldTest();
    //System.exit(0);
  }

  private static void retrieveFieldTest() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", NameServer.nodeID);
    HashFunction.initializeHashFunction();
    BasicRecordMap recordMap = new MongoRecordMap(MongoRecords.DBNAMERECORD);
    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", NameRecord.PRIMARY_NAMESERVERS));
    recordMap.updateNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED", new HashSet<Integer>(Arrays.asList(1, 2, 3)));
    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED"));
  }
}
