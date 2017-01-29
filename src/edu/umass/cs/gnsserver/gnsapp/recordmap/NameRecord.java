
package edu.umass.cs.gnsserver.gnsapp.recordmap;

import edu.umass.cs.gigapaxos.interfaces.Summarizable;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.database.ColumnField;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordExistsException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.JSONUtils;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;


public class NameRecord implements Comparable<NameRecord>, Summarizable {


  public final static ColumnField NAME = new ColumnField("nr_name", ColumnFieldType.STRING);

  public final static ColumnField VALUES_MAP = new ColumnField("nr_valuesMap", ColumnFieldType.VALUES_MAP);

  private final HashMap<ColumnField, Object> hashMap;

  private final BasicRecordMap recordMap;


  public NameRecord(BasicRecordMap recordMap, String name, ValuesMap values) {
    this.recordMap = recordMap;

    hashMap = new HashMap<>();
    hashMap.put(NAME, name);
    hashMap.put(VALUES_MAP, values);

  }


  public NameRecord(BasicRecordMap recordMap, JSONObject jsonObject) throws JSONException {
    this.recordMap = recordMap;
    hashMap = new HashMap<>();
    if (jsonObject.has(NAME.getName())) {
      hashMap.put(NAME, jsonObject.getString(NAME.getName()));
    }
    if (jsonObject.has(VALUES_MAP.getName())) {
      hashMap.put(VALUES_MAP, new ValuesMap(jsonObject.getJSONObject(VALUES_MAP.getName())));
    }
  }


  public NameRecord(BasicRecordMap recordMap, HashMap<ColumnField, Object> allValues) {
    this.recordMap = recordMap;
    this.hashMap = allValues;
  }


  public NameRecord(BasicRecordMap recordMap, String name) {
    this.recordMap = recordMap;
    hashMap = new HashMap<>();
    hashMap.put(NAME, name);
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }


  public String toReasonableString() {
    try {
      return toJSONObject().toReasonableString();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return null;
  }


  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    for (ColumnField f : hashMap.keySet()) {
      jsonObject.put(f.getName(), hashMap.get(f));
    }
    return jsonObject;
  }


  public String getName() throws FieldNotFoundException {
    if (hashMap.containsKey(NAME)) {
      return (String) hashMap.get(NAME);
    }
    throw new FieldNotFoundException(NAME);
  }


  public ValuesMap getValuesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      return (ValuesMap) hashMap.get(VALUES_MAP);
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }



  public boolean containsUserKey(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      return valuesMap.has(key);
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }


  public ResultValue getUserKeyAsArray(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      if (valuesMap.has(key)) {
        return valuesMap.getAsArray(key);
      }
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }


  public boolean updateNameRecord(String recordKey, ResultValue newValues, ResultValue oldValues, int argument,
          ValuesMap userJSON, UpdateOperation operation) throws FieldNotFoundException, FailedDBOperationException {

    // Handle special case for SINGLE_FIELD_REMOVE_FIELD operation
    // whose purpose is to remove the field with name = key from values map.
    if (operation.equals(UpdateOperation.SINGLE_FIELD_REMOVE_FIELD)) {
      
      ArrayList<ColumnField> keys = new ArrayList<>();
      keys.add(new ColumnField(recordKey, ColumnFieldType.LIST_STRING));
       GNSConfig.getLogger().log(Level.FINE,
                    "<============>REMOVE {0} from {1}<============>", new Object[]{recordKey, getName()});
      recordMap.removeMapKeys(getName(), VALUES_MAP, keys);
      return true;
    }


    ValuesMap valuesMap;
    if (operation.isAbleToSkipRead()) {
      valuesMap = new ValuesMap();
      hashMap.put(VALUES_MAP, valuesMap);
    } else {
      valuesMap = getValuesMap(); // this will throw an exception if field is not read.
    }
    // FIXME: might want to handle this without a special case at some point
    boolean updated = UpdateOperation.USER_JSON_REPLACE.equals(operation)
            || UpdateOperation.USER_JSON_REPLACE_OR_CREATE.equals(operation)
            ? true
            : UpdateOperation.updateValuesMap(valuesMap, recordKey, newValues, oldValues, argument, userJSON, operation);
    if (updated) {
      // commit updateEntireValuesMap to database
      ArrayList<ColumnField> updatedFields = new ArrayList<>();
      ArrayList<Object> updatedValues = new ArrayList<>();
      if (userJSON != null) {
        // full userJSON (new style) updateEntireValuesMap
        Iterator<?> keyIter = userJSON.keys();
        while (keyIter.hasNext()) {
          String key = (String) keyIter.next();
          try {
            updatedFields.add(new ColumnField(key, ColumnFieldType.USER_JSON));
            updatedValues.add(userJSON.get(key));
          } catch (JSONException e) {
            GNSConfig.getLogger().log(Level.SEVERE,
                    "Unable to get {0} from userJSON:{1}", new Object[]{key, e});
          }
        }
      } else {
        // single field updateEntireValuesMap
        updatedFields.add(new ColumnField(recordKey, ColumnFieldType.LIST_STRING));
        updatedValues.add(valuesMap.getAsArray(recordKey));
      }

      recordMap.updateIndividualFields(getName(), updatedFields, updatedValues);
    }
    return updated;
  }


  public void updateState(ValuesMap valuesMap) throws FieldNotFoundException, FailedDBOperationException {
    recordMap.updateEntireValuesMap(getName(), valuesMap);
    hashMap.put(VALUES_MAP, valuesMap);
  }

//BEGIN: static methods for reading/writing to database and iterating over records
//

  public static boolean containsRecord(BasicRecordMap recordMap, String name)
          throws FailedDBOperationException {
    return recordMap.containsName(name);
  }


  public static NameRecord getNameRecord(BasicRecordMap recordMap, String name)
          throws RecordNotFoundException, FailedDBOperationException {
    try {
      JSONObject json = recordMap.lookupEntireRecord(name);
      if (json != null) {
        return new NameRecord(recordMap, json);
      }
    } catch (JSONException e) {
      GNSConfig.getLogger().log(Level.SEVERE, "Error getting name record {0}: {1}", new Object[]{name, e});
    }
    return null;

  }


  public static NameRecord getNameRecordMultiUserFields(BasicRecordMap recordMap, String name,
          ColumnFieldType returnType, String... userFieldNames)
          throws RecordNotFoundException, FailedDBOperationException {
    return new NameRecord(recordMap, recordMap.lookupUserFields(name,
            NameRecord.NAME, NameRecord.VALUES_MAP,
            userFieldList(returnType, userFieldNames)));
  }

  private static ArrayList<ColumnField> userFieldList(ColumnFieldType returnType, String... fieldNames) {
    ArrayList<ColumnField> result = new ArrayList<>();
    for (String fieldName : fieldNames) {
      result.add(new ColumnField(fieldName, returnType));
    }
    return result;
  }


  public static void addNameRecord(BasicRecordMap recordMap, NameRecord record) throws JSONException, FailedDBOperationException, RecordExistsException {
    recordMap.addRecord(record.toJSONObject());
  }


  public static void removeNameRecord(BasicRecordMap recordMap, String name) throws FailedDBOperationException {
    recordMap.removeRecord(name);
  }


  public static AbstractRecordCursor getAllRowsIterator(BasicRecordMap recordMap) throws FailedDBOperationException {
    return recordMap.getAllRowsIterator();
  }


  public static AbstractRecordCursor selectRecords(BasicRecordMap recordMap, String key, Object value) throws FailedDBOperationException {
    return recordMap.selectRecords(NameRecord.VALUES_MAP, key, value);
  }


  public static AbstractRecordCursor selectRecordsWithin(BasicRecordMap recordMap, String key, String value) throws FailedDBOperationException {
    return recordMap.selectRecordsWithin(NameRecord.VALUES_MAP, key, value);
  }


  public static AbstractRecordCursor selectRecordsNear(BasicRecordMap recordMap, String key, String value, Double maxDistance) throws FailedDBOperationException {
    return recordMap.selectRecordsNear(NameRecord.VALUES_MAP, key, value, maxDistance);
  }


  public static AbstractRecordCursor selectRecordsQuery(BasicRecordMap recordMap, String query) throws FailedDBOperationException {
    return recordMap.selectRecordsQuery(NameRecord.VALUES_MAP, query);
  }


  @Override
  public int compareTo(NameRecord d) {
    int result;
    try {
      result = (this.getName()).compareTo(d.getName());
    } catch (FieldNotFoundException ex) {
      result = 0;
    }
    return result;
  }


  @Override
  public Object getSummary() {
    return Util.truncate(NameRecord.this, 64, 64);
  }

}
