/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.database;

import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import org.json.JSONObject;

/**
 * Provides an interface insert, update, remove and lookup operations in a nosql database
 *
 * @author westy
 */
public interface NoSQLRecords {
  public void reset(String collection);
  public JSONObject lookup(String collection, String guid) throws RecordNotFoundException;
  public String lookup(String collection, String guid, String key) throws RecordNotFoundException;
  public ArrayList<String> lookup(String collection, String guid, ArrayList<String> key);

  public void insert(String collection, String guid, JSONObject value) throws RecordExistsException;
  public void update(String collection, String guid, JSONObject value);
  public void updateField(String collection, String guid, String key, Object object);
  public boolean contains(String collection, String guid);
  public void remove(String collection, String guid);
  public ArrayList<JSONObject> retrieveAllEntries(String collection);
  public Set<String> keySet(String collection);
  public void printAllEntries(String collection);

  /**
   * For the record with given name, return the values of given fields in form of HashMap.
   * @param name
   * @param nameField
   * @param fields1
   * @return
   */
  public abstract HashMap<Field,Object> lookup(String collection, String name, Field nameField, ArrayList<Field> fields1) throws RecordNotFoundException;

  /**
   * For record with given name, return the values of given fields and from the values map field of the record,
   * return the values of given keys.
   * @param name
   * @param nameField
   * @param fields1
   * @param valuesMapField
   * @param valuesMapKeys
   * @return
   */
  public abstract HashMap<Field,Object> lookup(String collection, String name, Field nameField, ArrayList<Field> fields1,
                                               Field valuesMapField, ArrayList<Field> valuesMapKeys) throws RecordNotFoundException;

  /**
   * For the record with given name, replace the values of given fields to the given values
   * @param name
   * @param fields1
   * @param values1
   */
  public abstract void update(String collection, String name, Field nameField, ArrayList<Field> fields1, ArrayList<Object> values1);


  /**
   * For the record with given name, replace the values of given fields to the given values,
   * and the the values map field of the record, replace the values of given keys to the given values.
   * @param name
   * @param nameField
   * @param fields1
   * @param values1
   * @param valuesMapField
   * @param valuesMapKeys
   * @param valuesMapValues
   */
  public abstract void update(String collection, String name, Field nameField, ArrayList<Field> fields1, ArrayList<Object> values1,
                              Field valuesMapField, ArrayList<Field> valuesMapKeys, ArrayList<Object> valuesMapValues);

  /**
   * For the record with given name, increment the values of given fields by given values. (Another form of update).
   * @param name
   * @param fields1
   * @param values1
   */
  public abstract void increment(String collection, String name, ArrayList<Field> fields1, ArrayList<Object> values1);


  /**
   * For the record with given name, increment the values of given fields by given values.
   * In the votes map field, increment values of given keys by given values.
   * @param collectionName
   * @param guid
   * @param fields1
   * @param values1
   * @param votesMapField
   * @param votesMapKeys
   * @param votesMapValues
   */
  public void increment(String collectionName, String guid, ArrayList<Field> fields1, ArrayList<Object> values1,
                        Field votesMapField, ArrayList<Field> votesMapKeys, ArrayList<Object> votesMapValues);


  public Object getIterator(String collection, Field nameField, ArrayList<Field> fields);

  public HashMap<Field, Object> next(Object iterator, Field nameField, ArrayList<Field> fields);

  public BasicRecordCursor getAllRowsIterator(String collection);

  //public JSONObject next(Object iterator, Field nameField);

  //public void returnIterator();

//  public DBIterator getIterator(String collection);

  @Override
  public String toString();
 
}
