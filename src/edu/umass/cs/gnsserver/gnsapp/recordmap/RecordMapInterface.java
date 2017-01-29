
package edu.umass.cs.gnsserver.gnsapp.recordmap;

//import edu.umass.cs.gnsserver.nsdesign.recordmap.ReplicaControllerRecord;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;


public interface RecordMapInterface {


  public void createIndex(String field, String index);


  public void addRecord(JSONObject json) throws FailedDBOperationException, RecordExistsException;


  public JSONObject lookupEntireRecord(String name) throws RecordNotFoundException, FailedDBOperationException;


  public HashMap<ColumnField, Object> lookupUserFields(String name, ColumnField nameField,
          ColumnField valuesMapField, ArrayList<ColumnField> valuesMapKeys)
          throws RecordNotFoundException, FailedDBOperationException;


  public void removeRecord(String name) throws FailedDBOperationException;


  public boolean containsName(String name) throws FailedDBOperationException;

//
//   public HashMap<ColumnField, Object> lookupSystemFields(String name, ColumnField nameField, 
//          ArrayList<ColumnField> systemFields) throws RecordNotFoundException, FailedDBOperationException;
//   

  public abstract void updateEntireValuesMap(String name, ValuesMap valuesMap)
          throws FailedDBOperationException;


  public abstract void updateIndividualFields(String name, ArrayList<ColumnField> valuesMapKeys, ArrayList<Object> valuesMapValues)
          throws FailedDBOperationException;


  public abstract void removeMapKeys(String name, ColumnField mapField, ArrayList<ColumnField> mapKeys)
          throws FailedDBOperationException;


  public abstract AbstractRecordCursor getAllRowsIterator() throws FailedDBOperationException;


  public abstract AbstractRecordCursor selectRecords(ColumnField valuesMapField,
          String key, Object value) throws FailedDBOperationException;


  public abstract AbstractRecordCursor selectRecordsWithin(ColumnField valuesMapField,
          String key, String value) throws FailedDBOperationException;


  public abstract AbstractRecordCursor selectRecordsNear(ColumnField valuesMapField,
          String key, String value, Double maxDistance) throws FailedDBOperationException;


  public abstract AbstractRecordCursor selectRecordsQuery(ColumnField valuesMapField,
          String query) throws FailedDBOperationException;

}
