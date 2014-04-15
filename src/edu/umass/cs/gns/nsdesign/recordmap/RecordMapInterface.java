/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.nsdesign.recordmap;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 *
 * @author westy
 */
public interface RecordMapInterface {

  public void addNameRecord(NameRecord recordEntry) throws FailedUpdateException, RecordExistsException;

  public NameRecord getNameRecord(String name) throws RecordNotFoundException;

  public void updateNameRecord(NameRecord recordEntry) throws FailedUpdateException;

  public void addNameRecord(JSONObject json) throws FailedUpdateException, RecordExistsException;

  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws FailedUpdateException, RecordExistsException;

  public void removeNameRecord(String name) throws FailedUpdateException;

  public boolean containsName(String name);

  public Set<String> getAllColumnKeys(String key) throws RecordNotFoundException;

  /**
   * Clears the database and reinitializes all indices.
   */
  public void reset();

  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1) throws RecordNotFoundException;

  public HashMap<ColumnField, Object> lookup(String name, ColumnField nameField, ArrayList<ColumnField> fields1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException;

  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1)
          throws FailedUpdateException;

  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedUpdateException;

  /**
   * Updates the record indexed by name conditionally. The condition specified by 
   * conditionField whose value must be equal to conditionValue.
   * Didn't write this so not sure about all the other arguments.
   * 
   * @param name
   * @param nameField
   * @param conditionField
   * @param conditionValue
   * @param fields1
   * @param values1
   * @param valuesMapField
   * @param valuesMapKeys
   * @param valuesMapValues
   * @return Returns true if the update was applied false otherwise.
   * @throws FailedUpdateException 
   */
  public abstract boolean updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue,
          ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField,
          ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedUpdateException;

  public abstract void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1)
          throws FailedUpdateException;

  public abstract void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1,
          ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues)
          throws FailedUpdateException;

  public abstract void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedUpdateException;

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
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [LONG, LAT]
   * @param maxDistance - the distance in meters
   * @return
   */
  public abstract BasicRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance);

  public abstract BasicRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query);

  // Replica Controller
  public ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException;

  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) throws FailedUpdateException, RecordExistsException;

  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) throws FailedUpdateException;
}
