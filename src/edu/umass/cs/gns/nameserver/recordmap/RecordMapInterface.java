/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.nameserver.recordExceptions.RecordExistsException;
import edu.umass.cs.gns.nameserver.recordExceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import org.json.JSONObject;

import java.util.*;

/**
 *
 * @author westy
 */
public interface RecordMapInterface {

  public void addNameRecord(JSONObject json) throws RecordExistsException;

  public void removeNameRecord(String name);

  public boolean containsName(String name);

  public Set<String> getAllColumnKeys(String key) throws RecordNotFoundException;

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

  public HashMap<Field,Object> lookup(String name, Field nameField, ArrayList<Field> fields1) throws RecordNotFoundException;

  public HashMap<Field,Object> lookup(String name, Field nameField, ArrayList<Field> fields1,
                           Field valuesMapField, ArrayList<Field> valuesMapKeys) throws RecordNotFoundException;

  public abstract void update(String name, Field nameField, ArrayList<Field> fields1, ArrayList<Object> values1);


  public abstract void update(String name, Field nameField, ArrayList<Field> fields1, ArrayList<Object> values1,
                              Field valuesMapField, ArrayList<Field> valuesMapKeys, ArrayList<Object> valuesMapValues);

  public abstract void increment(String name, ArrayList<Field> fields1, ArrayList<Object> values1);

  public abstract void increment(String name, ArrayList<Field> fields1, ArrayList<Object> values1,
                                 Field votesMapField, ArrayList<Field> votesMapKeys, ArrayList<Object> votesMapValues);

  public abstract Object getIterator(Field nameField, ArrayList<Field> fields);

  public abstract HashMap<Field,Object> next(Object iterator, Field nameField, ArrayList<Field> fields);

  public abstract Object getIterator(Field nameField);

  public abstract JSONObject next(Object iterator, Field nameField);

  public abstract void returnIterator();
  //
  // OLD Style
  //
  public void addNameRecord(NameRecord recordEntry) throws RecordExistsException;

  public NameRecord getNameRecord(String name) throws RecordNotFoundException;

  public Set<NameRecord> getAllNameRecords();

  public void updateNameRecord(NameRecord recordEntry);

  // Replica Controller
  
  public ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException;

  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) throws RecordExistsException;

  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry);
  
  public Set<ReplicaControllerRecord> getAllPrimaryNameRecords();
  
  public ReplicaControllerRecord getNameRecordPrimaryLazy(String name);



  
}
