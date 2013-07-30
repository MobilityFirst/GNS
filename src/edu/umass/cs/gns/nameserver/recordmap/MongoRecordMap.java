package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.StartNameServer;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class MongoRecordMap extends BasicRecordMap {

  private final String DBNAMERECORD = MongoRecords.DBNAMERECORD;

  @Override
  public String getNameRecordField(String name, String key) {
    MongoRecords records = MongoRecords.getInstance();
    String result = records.lookup(DBNAMERECORD, name, key);
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
    records.updateListValue(DBNAMERECORD, name, key, value);
  }

  @Override
  public void updateNameRecordField(String name, String key, String string) {
    MongoRecords records = MongoRecords.getInstance();
    GNS.getLogger().fine(records.toString() + ":: Writing string " + name + "/" + key + ": " + string);
    records.updateField(DBNAMERECORD, name, key, string);
  }

  @Override
  public Set<String> getAllRowKeys() {
    MongoRecords records = MongoRecords.getInstance();
    return records.keySet(DBNAMERECORD);
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
    if (!containsName(name)) {
      try {
        MongoRecords records = MongoRecords.getInstance();
        JSONObject json = records.lookup(DBNAMERECORD, name);
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
  public NameRecord getNameRecord(String name) {
    try {
      JSONObject json = MongoRecords.getInstance().lookup(DBNAMERECORD, name);
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
      //MongoRecords.getInstance().insert(DBNAMERECORD, recordEntry.getName(), recordEntry.toJSONObject());
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
      records.insert(DBNAMERECORD, name, json);
      GNS.getLogger().finer(records.toString() + ":: Added " + name);
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecord(NameRecord recordEntry) {
    try {
      MongoRecords.getInstance().update(DBNAMERECORD, recordEntry.getName(), recordEntry.toJSONObject());
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Override
  public void removeNameRecord(String name) {
    MongoRecords.getInstance().remove(DBNAMERECORD, name);
  }

  @Override
  public boolean containsName(String name) {
    return MongoRecords.getInstance().contains(DBNAMERECORD, name);
  }

  @Override
  public Set<NameRecord> getAllNameRecords() {
    MongoRecords.getInstance().keySet(DBNAMERECORD);
    MongoRecords records = MongoRecords.getInstance();
    Set<NameRecord> result = new HashSet<NameRecord>();
    for (JSONObject json : records.retrieveAllEntries(DBNAMERECORD)) {
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
    MongoRecords.getInstance().reset();
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
    BasicRecordMap recordMap = new MongoRecordMap();
    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", NameRecord.PRIMARY_NAMESERVERS));
    recordMap.updateNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED", new HashSet<Integer>(Arrays.asList(1, 2, 3)));
    System.out.println(recordMap.getNameRecordFieldAsIntegerSet("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRED"));
  }
}
