package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * This will be deleted soon.
 * 
 * Stores GUID, KEY, VALUE triples in a Mongo Database
 *
 * @author westy
 */
public class MongoRecordMapOLD extends BasicRecordMap {
  String collectionName = MongoRecords.DBNAMERECORD;
  
  @Override
  public NameRecord getNameRecord(String name) {
    MongoRecords records = MongoRecords.getInstance();

    try {
      JSONObject jsonObject = records.lookup(collectionName,name);
      if (jsonObject != null) {
        GNS.getLogger().finer(records.toString() + ":: Retrieved " + name + ": " + jsonObject.toString());
        return new NameRecord(jsonObject);
      } else {
        GNS.getLogger().finer(records.toString() + ":: No record named " + name);
        return null;
      }
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error getting name record " + name + ": " + e);
      e.printStackTrace();
      return null;
    }
  }
  
  @Override
  public NameRecord getNameRecordLazy(String name) {
   throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void addNameRecord(NameRecord record) {
    MongoRecords records = MongoRecords.getInstance();
    try {
      records.insert(collectionName,record.getName(), record.toJSONObject());
      GNS.getLogger().finer(records.toString() + ":: Added " + record.getName());
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error adding name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void addNameRecord(JSONObject json) {
    MongoRecords records = MongoRecords.getInstance();
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
  public String getNameRecordField(String name, String string) {
    MongoRecords records = MongoRecords.getInstance();
    String result = records.lookup(collectionName, name, string);
    if (result != null) {
      GNS.getLogger().finer(records.toString() + ":: Retrieved " + name + "/" + string + ": " + result);
      return result;
    } else {
      GNS.getLogger().finer(records.toString() + ":: No record named " + name + " with key " + string);
      return null;
    }
  }

  @Override
  public void updateNameRecord(NameRecord record) {
    MongoRecords records = MongoRecords.getInstance();
    try {
      records.update(collectionName, record.getName(), record.toJSONObject());
      GNS.getLogger().finer(records.toString() + ":: Updated " + record.getName());
    } catch (JSONException e) {
      GNS.getLogger().severe(records.toString() + ":: Error updating name record: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void updateNameRecordListValue(String name, String string, ArrayList<String> value) {
    MongoRecords records = MongoRecords.getInstance();
    records.updateListValue(collectionName, name, string, value);
  }
  
  @Override
  public void updateNameRecordField(String name, String key, String string) {
    MongoRecords records = MongoRecords.getInstance();
    records.updateField(collectionName, name, key, string);
  }

  @Override
  public void removeNameRecord(String name) {
    MongoRecords records = MongoRecords.getInstance();
    records.remove(collectionName, name);
    GNS.getLogger().finer(records.toString() + ":: Removed " + name);
  }

  @Override
  public boolean containsName(String name) {
    MongoRecords records = MongoRecords.getInstance();
    return records.contains(collectionName, name);
  }

  @Override
  public Set<NameRecord> getAllNameRecords() {
    MongoRecords records = MongoRecords.getInstance();
    Set<NameRecord> result = new HashSet();
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
  public Set<String> getAllRowKeys() {
    MongoRecords records = MongoRecords.getInstance();
    return records.keySet(collectionName);
  }

  @Override
  public Set<String> getAllColumnKeys(String name) {
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
  public void reset() {
    MongoRecords.getInstance().reset();
  }

  //test code
  public static void main(String[] args) throws Exception {
    test();
  }

  private static void test() throws Exception {
    ConfigFileInfo.readHostInfo("ns1", 4);
    HashFunction.initializeHashFunction();
    MongoRecordMapOLD recordMap = new MongoRecordMapOLD();
    NameRecord nameRecord = new NameRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24",
            new NameRecordKey("FRANK"),
            new ArrayList(Arrays.asList("XYZ")));
    recordMap.addNameRecord(nameRecord);
    nameRecord = recordMap.getNameRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
    System.out.println(nameRecord);
    if (nameRecord != null) {
      System.out.println(nameRecord.get("_GNS_account_info"));
      System.out.println(nameRecord.get("_GNS_guid_info"));
    }
    System.out.println(recordMap.getNameRecordField("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRANK"));
    recordMap.updateNameRecordSingleValue("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRANK", "SLACKER");
    System.out.println(recordMap.getNameRecordField("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24", "FRANK"));
    System.out.println(recordMap.getAllRowKeys());
    nameRecord = recordMap.getNameRecord("1A434C0DAA0B17E48ABD4B59C632CF13501C7D24");
    System.out.println(nameRecord);
    if (nameRecord != null) {
      System.out.println(nameRecord.get("FRANK"));
    }
  }
}
