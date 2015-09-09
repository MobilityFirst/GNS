/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.recordmap;

import edu.umass.cs.gns.database.AbstractRecordCursor;
import edu.umass.cs.gns.database.ColumnField;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
//import edu.umass.cs.gns.nsdesign.recordmap.ReplicaControllerRecord;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 *
 * @author westy, abhigyan
 */
public interface RecordMapInterface {

  public void addNameRecord(NameRecord recordEntry) throws FailedDBOperationException, RecordExistsException;

  public NameRecord getNameRecord(String name) throws RecordNotFoundException, FailedDBOperationException;

  public void updateNameRecord(NameRecord recordEntry) throws FailedDBOperationException;

  public void addNameRecord(JSONObject json) throws FailedDBOperationException, RecordExistsException;

  public void bulkInsertRecords(ArrayList<JSONObject> jsons) throws FailedDBOperationException, RecordExistsException;

  public void removeNameRecord(String name) throws FailedDBOperationException;

  public boolean containsName(String name) throws FailedDBOperationException;

  public Set<String> getAllColumnKeys(String key) throws RecordNotFoundException, FailedDBOperationException;

  /**
   * Clears the database and reinitializes all indices.
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void reset() throws FailedDBOperationException;

  public HashMap<ColumnField, Object> lookupMultipleSystemFields(String name, ColumnField nameField, ArrayList<ColumnField> fields1) throws
          FailedDBOperationException, RecordNotFoundException;

  public HashMap<ColumnField, Object> lookupMultipleSystemAndUserFields(String name, ColumnField nameField, ArrayList<ColumnField> fields1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws
          FailedDBOperationException, RecordNotFoundException;

  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1)
          throws FailedDBOperationException;

  public abstract void update(String name, ColumnField nameField, ArrayList<ColumnField> fields1, ArrayList<Object> values1,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException;

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
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract boolean updateConditional(String name, ColumnField nameField, ColumnField conditionField, Object conditionValue,
          ArrayList<ColumnField> fields1, ArrayList<Object> values1, ColumnField valuesMapField,
          ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException;

  public abstract void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1)
          throws FailedDBOperationException;

  public abstract void increment(String name, ArrayList<ColumnField> fields1, ArrayList<Object> values1,
          ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues)
          throws FailedDBOperationException;

  public abstract void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException;

  /**
   * Returns an iterator for all the rows in the collection with only the columns in fields filled in except
   * the NAME (AKA the primary key) is always there.
   *
   * @param nameField - the field in the row that contains the name field
   * @param fields
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor getIterator(ColumnField nameField, ArrayList<ColumnField> fields) throws FailedDBOperationException;

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor getAllRowsIterator() throws FailedDBOperationException;

  /**
   * Given a key and a value return all the records as a AbstractRecordCursor that have a *user* key with that value.
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecords(ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified as a nested JSONArray
 string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a AbstractRecordCursor.
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]]
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsWithin(ColumnField valuesMapField, String key, String value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified as a JSONArray string tuple:
   * [LONG, LAT]. maxDistance is in radians. The returned value is a AbstractRecordCursor.
   *
   * @param valuesMapField - the field in the row that contains the *user* fields
   * @param key
   * @param value - a string that looks like this [LONG, LAT]
   * @param maxDistance - the distance in meters
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract AbstractRecordCursor selectRecordsNear(ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException;

  public abstract AbstractRecordCursor selectRecordsQuery(ColumnField valuesMapField, String query) throws FailedDBOperationException;

  // Replica Controller
//  @Deprecated
//  public ReplicaControllerRecord getNameRecordPrimary(String name) throws RecordNotFoundException, FailedDBOperationException;
//
//  @Deprecated
//  public void addNameRecordPrimary(ReplicaControllerRecord recordEntry) throws FailedDBOperationException, RecordExistsException;
//
//  @Deprecated
//  public void updateNameRecordPrimary(ReplicaControllerRecord recordEntry) throws FailedDBOperationException;
}
