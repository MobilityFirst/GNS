/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nameserver.recordmap;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.replicacontroller.ReplicaControllerRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public interface RecordMapInterface {

  public void addNameRecord(NameRecord recordEntry) throws RecordExistsException;

  public NameRecord getNameRecord(String name) throws RecordNotFoundException;

  public void updateNameRecord(NameRecord recordEntry);

  public void addNameRecord(JSONObject json) throws RecordExistsException;

  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws RecordExistsException;

  public void removeNameRecord(String name);

  public boolean containsName(String name);

  public Set<String> getAllColumnKeys(String key) throws RecordNotFoundException;

  public Set<String> getAllRowKeys();

  public void reset();

  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1) throws RecordNotFoundException;

  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException;

  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1);

  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues);

  public abstract void updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue,
                               ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField,
                               ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues);

  public abstract void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1);

  public abstract void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1,
          ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues);

  public abstract void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys);
  /**
   * Returns an iterator for all the rows in the collection with only the columns in fields filled in except
   * the NAME (AKA the primary key) is always there.
   * 
   * @param nameField - the field in the row that contains the name field
   * @param fields
   * @return 
   */
  public abstract BasicRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields);

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   * 
   * @return 
   */
  public abstract BasicRecordCursor getAllRowsIterator();

  /**
   * Given a key and a value return all the records as a BasicRecordCursor that have a *user* key with that value.
   * 
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value
   * @return 
   */
  public abstract BasicRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value);

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified as a nested JSONArray
   * string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a BasicRecordCursor.
   * 
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return 
   */
  public abstract BasicRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value);

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified as a JSONArray string tuple: 
   * [LONG, LAT]. maxDistance is in radians. The returned value is a BasicRecordCursor.
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [LONG, LAT]
   * @param maxDistance - the distance in radians
   * @return 
   */
  public abstract BasicRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance);
  
  public abstract BasicRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query);

  // Replica Controller
  public ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException;

  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) throws RecordExistsException;

  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry);
}
