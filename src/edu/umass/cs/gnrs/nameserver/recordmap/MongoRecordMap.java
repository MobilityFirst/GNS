package edu.umass.cs.gnrs.nameserver.recordmap;

import edu.umass.cs.gnrs.database.MongoRecords;
import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.main.StartNameServer;
import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gnrs.nameserver.NameRecord;
import edu.umass.cs.gnrs.util.JSONUtils;
import java.util.ArrayList;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class MongoRecordMap extends BasicRecordMap {

  private final String DBNAMERECORD = MongoRecords.DBNAMERECORD;
  
  @Override
  public String getNameRecordField(String name, String string) {
    MongoRecords records = MongoRecords.getInstance();
    String result = records.lookup(DBNAMERECORD, name, string);
    if (result != null) {
      GNS.getLogger().finer(records.toString() + ":: Retrieved " + name + "/" + string + ": " + result);
      return result;
    } else {
      GNS.getLogger().finer(records.toString() + ":: No record named " + name + " with key " + string);
      return null;
    }
  }
  
   @Override
  public void updateNameRecordField(String name, String string, ArrayList<String> value) {
    MongoRecords records = MongoRecords.getInstance();
    records.update(DBNAMERECORD, name, string, value);
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
}
