/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Provides an interface for insert, update, remove and lookup operations in a nosql database.
 * 
 * In some of the methods below we're going to make a distinction between *user* fields and *system* fields.
 * User fields are all stored in a single system field which we call the valuesMapField. Some of the methods
 * below let you read and write user fields just by specifying the short, unqualified name of the user field.
 * You could always access the fully qualified field if you know the magic syntax.
 * 
 * And to make things more confusing some of the *user* fields are actually only used internally by the GNS.
 *
 * @author westy, Abhigyan
 */
public interface NoSQLRecords {
  
  /**
   * Create a new record (row) with the given name using the JSONObject.
   *
   * @param collection
   * @param name
   * @param value
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   * @throws edu.umass.cs.gns.exceptions.RecordExistsException
   */
  public void insert(String collection, String name, JSONObject value) throws FailedDBOperationException, RecordExistsException;

  /**
   * Do a bulk insert of a bunch of documents into the database.
   *
   * @param collection collection to be inserted into.
   * @param values list of records to be inserted
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   * @throws edu.umass.cs.gns.exceptions.RecordExistsException
   */
  public void bulkInsert(String collection, ArrayList<JSONObject> values) throws FailedDBOperationException, RecordExistsException;

  /**
   * For the record with given name, return the entire record as a JSONObject.
   *
   * @param collection
   * @param name
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   * @throws RecordNotFoundException
   */
  public JSONObject lookupEntireRecord(String collection, String name) throws FailedDBOperationException,RecordNotFoundException;

  /**
   * For the record with given name, return the values of given fields in form of a HashMap.
   *
   * @param collection
   * @param name
   * @param nameField
   * @param fields
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public abstract HashMap<ColumnField, Object> lookupMultipleSystemFields(String collection, String name,
                                                      ColumnField nameField, ArrayList<ColumnField> fields) throws FailedDBOperationException, RecordNotFoundException;

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
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   */
  public abstract HashMap<ColumnField, Object> lookupMultipleSystemAndUserFields(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields,
                                                      ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys) throws FailedDBOperationException, RecordNotFoundException;

  /**
   * Returns true if a record with the given name exists, false otherwise.
   *
   * @param collection
   * @param name
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public boolean contains(String collection, String name) throws FailedDBOperationException;


  /**
   * Remove the record with the given name.
   *
   * @param collection
   * @param name
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void removeEntireRecord(String collection, String name) throws FailedDBOperationException;


  /**
   * Update the record (row) with the given name using the JSONObject.
   *
   * @param collection
   * @param name
   * @param value
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void update(String collection, String name, JSONObject value) throws FailedDBOperationException;

  /**
   * Update one field indexed by the key in the record (row) with the given name using the object.
   *
   * @param collection
   * @param name
   * @param key
   * @param object
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void updateField(String collection, String name, String key, Object object) throws FailedDBOperationException;


  /**
   * For the record with given name, replace the values of given fields to the given values.
   *
   * @param collection
   * @param name
   * @param nameField
   * @param fields
   * @param values
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract void update(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields,
          ArrayList<Object> values) throws FailedDBOperationException;

  /**
   * For the record with given name, replace the values of given fields to the given values,
   * and in the values map field of the record, replace the values of given keys to the given values.
   *
   * @param collection
   * @param name
   * @param nameField
   * @param fields
   * @param values
   * @param valuesMapField
   * @param valuesMapKeys
   * @param valuesMapValues
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract void update(String collection, String name, ColumnField nameField, ArrayList<ColumnField> fields,
          ArrayList<Object> values, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException;

  /**
   * Updates the record indexed by name conditionally. The condition specified by 
   * conditionField whose value must be equal to conditionValue.
   * Didn't write this so not sure about all the other arguments.
   * 
   * @param collectionName
   * @param guid
   * @param nameField
   * @param conditionField
   * @param conditionValue
   * @param fields
   * @param values
   * @param valuesMapField
   * @param valuesMapKeys
   * @param valuesMapValues
   * @return Returns true if the update happened, false otherwise.
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract boolean updateConditional(String collectionName, String guid, ColumnField nameField,
          ColumnField conditionField, Object conditionValue, ArrayList<ColumnField> fields, ArrayList<Object> values,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException;

  /**
   * For the record with given name, increment the values of given fields by given values. (Another form of update).
   *
   * @param collection
   * @param name
   * @param fields
   * @param values
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public abstract void increment(String collection, String name, ArrayList<ColumnField> fields,
          ArrayList<Object> values) throws FailedDBOperationException;

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
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void increment(String collectionName, String name, ArrayList<ColumnField> fields, ArrayList<Object> values,
          ColumnField votesMapField, ArrayList<ColumnField> votesMapKeys, ArrayList<Object> votesMapValues)
          throws FailedDBOperationException;

  /**
   * For record with name, removes (unset) keys in list <code>mapKeys</code> from the map <code>mapField</code>.
   *
   * @param collectionName
   * @param name
   * @param mapField
   * @param mapKeys
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void removeMapKeys(String collectionName, String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException;

  /**
   * Returns an iterator for all the rows in the collection with only the columns in fields filled in except
   * the NAME (AKA the primary key) is always there.
   *
   * @param collection
   * @param nameField
   * @param fields
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public AbstractRecordCursor getAllRowsIterator(String collection, ColumnField nameField, ArrayList<ColumnField> fields) throws FailedDBOperationException;

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @param collection
   * @return AbstractRecordCursor
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public AbstractRecordCursor getAllRowsIterator(String collection) throws FailedDBOperationException;

  /**
   * Given a key and a value return all the records as a AbstractRecordCursor that have a *user* key with that value.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @return AbstractRecordCursor
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public AbstractRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field returns all guids that are within value which is a bounding box specified as a nested JSONArray
 string tuple of paired tuples: [[LONG_UL, LAT_UL],[LONG_BR, LAT_BR]] The returned value is a AbstractRecordCursor.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @return AbstractRecordCursor
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public AbstractRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value) throws FailedDBOperationException;

  /**
   * If key is a GeoSpatial field returns all guids that are near value which is a point specified as a JSONArray string tuple:
   * [LONG, LAT]. maxDistance is in meters. The returned value is a AbstractRecordCursor.
   *
   * @param collectionName
   * @param valuesMapField
   * @param key
   * @param value
   * @param maxDistance
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public AbstractRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value, Double maxDistance) throws FailedDBOperationException;

  /**
   * Performs a query on the database and returns all guids that satisfy the query.
   * 
   * @param collectionName
   * @param valuesMapField
   * @param query
   * @return 
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException 
   */
  public AbstractRecordCursor selectRecordsQuery(String collectionName, ColumnField valuesMapField, String query) throws FailedDBOperationException;

  /**
   * Sets the collection back to an initial empty state with indexes also initialized.
   *
   * @param collection
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void reset(String collection) throws FailedDBOperationException;

 
  /** 
   * Return a string representation of the record set.
   * @return 
   */
   @Override
  public String toString();

  /**
   * Print all the records (ONLY FOR DEBUGGING).
   *
   * @param collection
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public void printAllEntries(String collection) throws FailedDBOperationException;

}
