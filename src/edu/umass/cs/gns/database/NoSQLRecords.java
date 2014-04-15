/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * ************** FIXME All functionality of this package is provided currently by class nsdesign/recordMap/MongoRecords.java.
 * FIXME Make changes to that file until we include this package again.. *
 */
/**
 * Provides an interface for insert, update, remove and lookup operations in a nosql database
 *
 * @author westy
 */
public interface NoSQLRecords {

  /**
   * For the record with given name, return the entire record as a JSONObject.
   *
   * @param collection
   * @param name
   * @return
   * @throws RecordNotFoundException
   */
  public JSONObject lookup(String collection, String name) throws RecordNotFoundException;

  /**
   * In the record with given name, return the value of the field (column) with the given key.
   *
   * @param collection
   * @param name
   * @param key
   * @return
   * @throws RecordNotFoundException
   */
  public String lookup(String collection, String name, String key) throws RecordNotFoundException;

  /**
   * In the record with given name, return the value of the fields (columns) with the given keys.
   *
   * @param collection
   * @param name
   * @param key
   * @return
   */
  public ResultValue lookup(String collection, String name, ArrayList<String> key);

  /**
   * Create a new record (row) with the given name using the JSONObject.
   *
   * @param collection
   * @param name
   * @param value
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void insert(String collection, String name, JSONObject value) throws FailedUpdateException;

  /**
   * Do a bulk insert of all documents into the database.
   *
   * @param collection collection to be inserted into.
   * @param values list of records to be inserted
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void bulkInsert(String collection, ArrayList<JSONObject> values) throws FailedUpdateException;

  /**
   * Update the record (row) with the given name using the JSONObject.
   *
   * @param collection
   * @param name
   * @param value
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void update(String collection, String name, JSONObject value) throws FailedUpdateException;

  /**
   * Update one field indexed by the key in the record (row) with the given name using the object.
   *
   * @param collection
   * @param name
   * @param key
   * @param object
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void updateField(String collection, String name, String key, Object object) throws FailedUpdateException;

  /**
   * Returns true if a record with the given name exists, false otherwise.
   *
   * @param collection
   * @param name
   * @return
   */
  public boolean contains(String collection, String name);

  /**
   * Remove the record with the given name.
   *
   * @param collection
   * @param name
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void remove(String collection, String name) throws FailedUpdateException;

  /**
   * Return a set of names of all the records.
   *
   * @param collection
   * @return
   */
  public Set<String> keySet(String collection);

  /**
   * For the record with given name, return the values of given fields in form of HashMap.
   *
   * @param name
   * @param nameField
   * @param fields
   * @return
   */
  public abstract HashMap<ColumnField, Object> lookup(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields) throws RecordNotFoundException;

  /**
   * For record with given name, return the values of given fields and from the values map field of the record,
   * return the values of given keys as a HashMap.
   *
   * @param collection
   * @param name
   * @param nameField
   * @param fields
   * @param valuesMapField
   * @param valuesMapKeys
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public abstract HashMap<ColumnField, Object> lookup(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws RecordNotFoundException;

  /**
   * For the record with given name, replace the values of given fields to the given values.
   *
   * @param collection
   * @param name
   * @param nameField
   * @param fields
   * @param values
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public abstract void update(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields,
          ArrayList<Object> values) throws FailedUpdateException;

  /**
   * For the record with given name, replace the values of given fields to the given values,
   * and the the values map field of the record, replace the values of given keys to the given values.
   *
   * @param collection
   * @param name
   * @param nameField
   * @param fields
   * @param values
   * @param valuesMapField
   * @param valuesMapKeys
   * @param valuesMapValues
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public abstract void update(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields,
          ArrayList<Object> values, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedUpdateException;

  public abstract void updateConditional(String collectionName, String guid, ColumnField nameField,
          ColumnField conditionField, Object conditionValue, ArrayList<ColumnField> fields, ArrayList<Object> values,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedUpdateException;

  /**
   * For the record with given name, increment the values of given fields by given values. (Another form of update).
   *
   * @param collection
   * @param name
   * @param fields
   * @param values
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public abstract void increment(String collection, String name, ArrayList<ColumnField> fields,
          ArrayList<Object> values) throws FailedUpdateException;

  /**
   * For the record with given name, increment the values of given fields by given values.
   * In the votes map field, increment values of given keys by given values.
   *
   * @param collectionName
   * @param name
   * @param fields
   * @param values
   * @param votesMapField
   * @param votesMapKeys
   * @param votesMapValues
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void increment(String collectionName, String name, ArrayList<ColumnField> fields, ArrayList<Object> values,
          ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues)
          throws FailedUpdateException;

  /**
   * For record with name, removes (unset) keys in list <code>mapKeys</code> from the map <code>mapField</code>.
   *
   * @param collectionName
   * @param name
   * @param mapField
   * @param mapKeys
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void removeMapKeys(String collectionName, String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedUpdateException;

  /**
   * Returns an iterator for all the rows in the collection with only the columns in fields filled in except
   * the NAME (AKA the primary key) is always there.
   *
   * @param collection
   * @param nameField
   * @param fields
   * @return
   */
  public BasicRecordCursor getAllRowsIterator(String collection, ColumnField nameField, ArrayList<ColumnField> fields);

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @param collection
   * @return BasicRecordCursor
   */
  public BasicRecordCursor getAllRowsIterator(String collection);

  /**
   * Given a key and a value return all the records as a BasicRecordCursor that have a *user* key with that value.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @return BasicRecordCursor
   */
  public BasicRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value);

  /**
   * If key is a GeoSpatial field return all fields that are within value which is a bounding box specified as a nested JSONArray
   * string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a BasicRecordCursor.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @return BasicRecordCursor
   */
  public BasicRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value);

  /**
   * If key is a GeoSpatial field return all fields that are near value which is a point specified as a JSONArray string tuple:
   * [LONG, LAT]. maxDistance is in meters. The returned value is a BasicRecordCursor.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @param maxDistance
   * @return
   */
  public BasicRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value, Double maxDistance);

  public BasicRecordCursor selectRecordsQuery(String collectionName, ColumnField valuesMapField, String query);

  /**
   * Puts the database in a state where it has nothing in it.
   *
   * @param collection
   */
  public void reset(String collection);

  @Override
  public String toString();

  /**
   * Print all the records (ONLY FOR DEBUGGING).
   *
   * @param collection
   */
  public void printAllEntries(String collection);

}
