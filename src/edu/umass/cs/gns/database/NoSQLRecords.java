/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.database;

import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.json.JSONObject;

/**
 * Provides an interface for insert, update, remove and lookup operations in a nosql database
 *
 * @author westy
 */
public interface NoSQLRecords {

  /**
   * For the record with given name, return the entire record as a JSONObject.
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
   * @throws RecordExistsException 
   */
  public void insert(String collection, String name, JSONObject value) throws RecordExistsException;

  /**
   * Update the record (row) with the given name using the JSONObject.
   * 
   * @param collection
   * @param name
   * @param value 
   */
  public void update(String collection, String name, JSONObject value);

  /**
   * Update one field indexed by the key in the record (row) with the given name using the object.
   * 
   * @param collection
   * @param name
   * @param key
   * @param object 
   */
  public void updateField(String collection, String name, String key, Object object);

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
   */
  public void remove(String collection, String name);

  /**
   * Return a set of names of all the records.
   * 
   * @param collection
   * @return 
   */
  public Set<String> keySet(String collection);

  /**
   * For the record with given name, return the values of given fields in form of HashMap.
   * @param name
   * @param nameField
   * @param fields
   * @return
   */
  public abstract HashMap<Field, Object> lookup(String collection, String name, Field nameField, ArrayList<Field> fields) throws RecordNotFoundException;

  /**
   * For record with given name, return the values of given fields and from the values map field of the record,
   * return the values of given keys as a HashMap.
   * @param name
   * @param nameField
   * @param fields
   * @param valuesMapField
   * @param valuesMapKeys
   * @return
   */
  public abstract HashMap<Field, Object> lookup(String collection, String name, Field nameField, ArrayList<Field> fields,
          Field valuesMapField, ArrayList<Field> valuesMapKeys) throws RecordNotFoundException;

  /**
   * For the record with given name, replace the values of given fields to the given values.
   * @param name
   * @param fields
   * @param values
   */
  public abstract void update(String collection, String name, Field nameField, ArrayList<Field> fields, ArrayList<Object> values);

  /**
   * For the record with given name, replace the values of given fields to the given values,
   * and the the values map field of the record, replace the values of given keys to the given values.
   * @param name
   * @param nameField
   * @param fields
   * @param values
   * @param valuesMapField
   * @param valuesMapKeys
   * @param valuesMapValues
   */
  public abstract void update(String collection, String name, Field nameField, ArrayList<Field> fields, ArrayList<Object> values,
          Field valuesMapField, ArrayList<Field> valuesMapKeys, ArrayList<Object> valuesMapValues);

  /**
   * For the record with given name, increment the values of given fields by given values. (Another form of update).
   * @param name
   * @param fields
   * @param values
   */
  public abstract void increment(String collection, String name, ArrayList<Field> fields, ArrayList<Object> values);

  /**
   * For the record with given name, increment the values of given fields by given values.
   * In the votes map field, increment values of given keys by given values.
   * @param collectionName
   * @param name
   * @param fields
   * @param values
   * @param votesMapField
   * @param votesMapKeys
   * @param votesMapValues
   */
  public void increment(String collectionName, String name, ArrayList<Field> fields, ArrayList<Object> values,
          Field votesMapField, ArrayList<Field> votesMapKeys, ArrayList<Object> votesMapValues);

  /**
   * Returns an iterator for all the rows in the collection with only the columns in fields filled in except
   * the NAME (AKA the primary key) is always there.
   * 
   * @param collectionName
   * @param nameField
   * @param fields
   * @return 
   */
  public BasicRecordCursor getAllRowsIterator(String collection, Field nameField, ArrayList<Field> fields);

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   * 
   * @param collectionName
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
  public BasicRecordCursor queryUserField(String collectionName, Field valuesMapField, String key, Object value);
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
   * @param collection 
   */
  public void printAllEntries(String collection);
  
}
