
package edu.umass.cs.gnsserver.database;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;


// Fixme: This interface needs to be cleaned up lot and made more consistent.
// In particular there is an extra layer of bullshit here. Many of the
// calls return a JSONObject (or equivalent - hence the inconsistency) that CONTAINS
// a ValuesMap instead of just returning JSONObject that IS a ValuesMap. 
// Someone with courage needs to fix this unnecessary design element that was
// added way back when by someone else. 
public interface NoSQLRecords {


  public void insert(String collection, String name, JSONObject value)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException,
          edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;


  public JSONObject lookupEntireRecord(String collection, String name)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException,
          edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;


  // FIXME: Why does this still have valuesMapField
  public HashMap<ColumnField, Object> lookupSomeFields(String collectionName,
          String guid, ColumnField nameField, ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException;


  public boolean contains(String collection, String name) throws
          edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public void removeEntireRecord(String collection, String name) throws
          edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public abstract void updateEntireRecord(String collection, String name,
          ValuesMap valuesMap) throws
          edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public void updateIndividualFields(String collectionName, String guid,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys,
          ArrayList<Object> valuesMapValues) throws FailedDBOperationException;


  public void removeMapKeys(String collectionName, String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public AbstractRecordCursor getAllRowsIterator(String collection)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public AbstractRecordCursor selectRecords(String collectionName, ColumnField valuesMapField, String key, Object value)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public AbstractRecordCursor selectRecordsWithin(String collectionName, ColumnField valuesMapField, String key, String value)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public AbstractRecordCursor selectRecordsNear(String collectionName, ColumnField valuesMapField, String key, String value, Double maxDistance)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public AbstractRecordCursor selectRecordsQuery(String collection, ColumnField valuesMapField, String query)
          throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;


  public void createIndex(String collectionName, String field, String index);


  @Override
  public String toString();


  public void printAllEntries(String collection) throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

}
