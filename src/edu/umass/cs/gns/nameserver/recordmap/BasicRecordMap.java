/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.StatsInfo;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import edu.umass.cs.gns.util.JSONUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public abstract class BasicRecordMap implements RecordMapInterface {

  public ArrayList<String> getNameRecordFieldAsArrayList(String name, String key) {
    String result = getNameRecordField(name, key);
    if (result != null) {
      try {
        return JSONUtils.JSONArrayToArrayList(new JSONArray(result));
      } catch (JSONException e) {
        GNS.getLogger().finer("Error parsing result: " + name + " with key " + key + " :" + e);
        return null;
      }
    } else {
      return null;
    }
  }

  public Set<Integer> getNameRecordFieldAsIntegerSet(String name, String key) {
    String result = getNameRecordField(name, key);
    if (result != null) {
      try {
        return JSONUtils.JSONArrayToSetInteger(new JSONArray(result));
      } catch (JSONException e) {
        GNS.getLogger().finer("Error parsing result: " + name + " with key " + key + " :" + e);
        return null;
      }
    } else {
      return null;
    }
  }

  public int getNameRecordFieldAsInt(String name, String key) {
    String result = getNameRecordField(name, key);
    if (result != null) {
      try {
        return Integer.parseInt(result);
      } catch (NumberFormatException e) {
        GNS.getLogger().finer("Error parsing result " + name + " with key " + key + " :" + e);
        return -1;
      }
    } else {
      return -1;
    }
  }

  public boolean getNameRecordFieldAsBoolean(String name, String key) {
    String result = getNameRecordField(name, key);
    if (result != null) {
      try {
        return Boolean.parseBoolean(result);
      } catch (NumberFormatException e) {
        GNS.getLogger().finer("Error parsing result " + name + " with key " + key + " :" + e);
        return false;
      }
    } else {
      return false;
    }
  }

  public ValuesMap getNameRecordFieldAsValuesMap(String name, String key) {
    String result = getNameRecordField(name, key);
    if (result != null) {
      try {
        return new ValuesMap(new JSONObject(result));
      } catch (JSONException e) {
        GNS.getLogger().finer("Error parsing result " + name + " with key " + key + " :" + e);
        return null;
      }
    } else {
      return null;
    }
  }

  public ConcurrentMap<Integer, StatsInfo> getNameRecordFieldAsStatsMap(String name, String key) {
    String result = getNameRecordField(name, key);
    if (result != null) {
      try {
        return new ConcurrentHashMap<Integer, StatsInfo>(ReplicaControllerRecord.toStatsMap(new JSONObject(result)));
      } catch (JSONException e) {
        GNS.getLogger().finer("Error parsing result " + name + " with key " + key + " :" + e);
        return null;
      }
    } else {
      return null;
    }
  }

  public ConcurrentMap<Integer, Integer> getNameRecordFieldAsVotesMap(String name, String key) {
    String result = getNameRecordField(name, key);
    if (result != null) {
      try {
        return new ConcurrentHashMap<Integer, Integer>(ReplicaControllerRecord.toIntegerMap(new JSONObject(result)));
      } catch (JSONException e) {
        GNS.getLogger().finer("Error parsing result " + name + " with key " + key + " :" + e);
        return null;
      }
    } else {
      return null;
    }
  }

  public void updateNameRecordSingleValue(String name, String key, String value) {
    updateNameRecordListValue(name, key, new ArrayList(Arrays.asList(value)));
  }

  public void updateNameRecordFieldAsInteger(String name, String key, int value) {
    updateNameRecordField(name, key, Integer.toString(value));
  }

  public void updateNameRecordFieldAsBoolean(String name, String key, boolean value) {
    updateNameRecordField(name, key, Boolean.toString(value));
  }

  public void updateNameRecordFieldAsIntegerSet(String name, String key, Set<Integer> value) {
    updateNameRecordField(name, key, new JSONArray(value).toString());
  }

  @Override
  public String tableToString() {
    StringBuilder table = new StringBuilder();
    for (String row : getAllRowKeys()) {
      table.append(row + ": ");
      String prefix = "";
      for (String column : getAllColumnKeys(row)) {
        table.append(prefix);
        table.append(column);
        table.append(" -> ");
        table.append(getNameRecordFieldAsArrayList(row, column));
        prefix = ", ";
      }
      table.append("\n");
    }
    return table.toString();
  }

}
