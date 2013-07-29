/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.database;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordV1;
import java.util.ArrayList;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides insert, update, remove and lookup operations for guid, key, record triples using JSONObjects as the
 * intermediate representation
 *
 * @author westy
 */
public class JAVAPreferencesRecords {

  private static Preferences preferences = Preferences.userRoot().node(JAVAPreferencesRecords.class.getName());

  public static JAVAPreferencesRecords getInstance() {
    return JAVAPreferencesRecordsHolder.INSTANCE;
  }

  private static class JAVAPreferencesRecordsHolder {

    private static final JAVAPreferencesRecords INSTANCE = new JAVAPreferencesRecords();
  }

  private JAVAPreferencesRecords() {
  }

  public JSONObject lookup(String guid, String key) {
    String result = preferences.get(guid + key, null);
    if (result != null) {
      try {
        return new JSONObject(result);
      } catch (JSONException e) {
        GNS.getLogger().warning("Unable to parse JSON: " + e);
        return null;
      }
    } else {
      return null;
    }

  }

  public void insert(String guid, String key, JSONObject value) {
    preferences.put(guid + key, value.toString());
  }

  public void update(String guid, String key, JSONObject value) {
    preferences.put(guid + key, value.toString());
  }

  public boolean contains(String guid, String key) {
    String result = preferences.get(guid + key, null);
    if (result != null) {
      return true;
    } else {
      return false;
    }
  }

  public void remove(String guid, String key) {
    preferences.remove(guid + key);
  }

  // punt on this one for now
  public void delete(String guid) {
  }

  public ArrayList<JSONObject> retrieveAllEntries() {
    ArrayList<JSONObject> result = new ArrayList<JSONObject>();
    try {
      for (String key : preferences.keys()) {
        try {
          result.add(new JSONObject(preferences.get(key, null)));
        } catch (JSONException e) {
          GNS.getLogger().warning("Unable to parse JSON: " + e);
        }
      }
    } catch (BackingStoreException e) {
      GNS.getLogger().warning("Problem retreiving records: " + e);
    }
    return result;
  }

  public void printAllEntries() {
    for (JSONObject entry : retrieveAllEntries()) {
      System.out.println(entry.toString());
    }
  }

  public void deleteEverything() {
    try {
      preferences.clear();
    } catch (BackingStoreException e) {
      GNS.getLogger().warning("Problem clearing records: " + e);
    }
  }

  // test code
  public static void main(String[] args) throws Exception {
    NameRecordV1 n = NameRecordV1.testCreateNameRecord();
    JAVAPreferencesRecords.getInstance().delete(n.getName());
    JAVAPreferencesRecords.getInstance().insert(n.getName(), n.getRecordKey().getName(), n.toJSONObject());
    System.out.println("LOOKUP =>" + JAVAPreferencesRecords.getInstance().lookup(n.getName(), n.getRecordKey().getName()));
    System.out.println("DUMP =v");
    JAVAPreferencesRecords.getInstance().printAllEntries();
  }
}
