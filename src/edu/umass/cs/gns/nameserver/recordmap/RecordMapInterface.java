/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author westy
 */
public interface RecordMapInterface {

  public void addNameRecord(JSONObject json);

  public void removeNameRecord(String name);

  public boolean containsName(String name);

  public Set<String> getAllColumnKeys(String key);

  public Set<String> getAllRowKeys();

  public void updateNameRecordListValue(String name, String key, ArrayList<String> value);

  public void updateNameRecordListValueInt(String name, String key, Set<Integer> value);

  public void updateNameRecordFieldAsString(String name, String key, String string);
  
  public void updateNameRecordFieldAsMap(String name, String key, Map map);
  
  public void updateNameRecordFieldAsCollection(String name, String key, Collection collection);

  public String getNameRecordField(String name, String key);

  public ArrayList<String> getNameRecordFields(String name, ArrayList<String> keys);

  public String tableToString();

  public void reset();

  public NameRecord getNameRecordLazy(String name);

  public NameRecord getNameRecordLazy(String name, ArrayList<String> keys);

  //
  // OLD Style
  //
  public void addNameRecord(NameRecord recordEntry);

  public NameRecord getNameRecord(String name);

  public Set<NameRecord> getAllNameRecords();

  public void updateNameRecord(NameRecord recordEntry);

  // Replica Controller
  
  public ReplicaControllerRecord getNameRecordPrimary(String name);

  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry);

  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry);
  
  public Set<ReplicaControllerRecord> getAllPrimaryNameRecords();
  
  public ReplicaControllerRecord getNameRecordPrimaryLazy(String name);
  
}
