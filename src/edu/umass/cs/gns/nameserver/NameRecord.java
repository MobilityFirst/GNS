package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.nameserver.fields.Field;
import edu.umass.cs.gns.nameserver.fields.FieldType;
import edu.umass.cs.gns.nameserver.recordExceptions.FieldNotFoundException;
import edu.umass.cs.gns.packet.UpdateOperation;
import edu.umass.cs.gns.util.HashFunction;
import edu.umass.cs.gns.util.JSONUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

//import edu.umass.cs.gns.packet.QueryResultValue;

/**
 * 
 * PLEASE DO NOT DELETE THE implements Comparable<NameRecord> BELOW. IT IS NECESSARY!!!! - Westy
 */
public class NameRecord implements Comparable<NameRecord> {
  
  public final static Field NAME = new Field("nr_name", FieldType.STRING);

  public final static Field ACTIVE_NAMESERVERS = new Field("nr_active", FieldType.SET_INTEGER);

  public final static Field PRIMARY_NAMESERVERS = new Field("nr_primary", FieldType.SET_INTEGER);

  public final static Field ACTIVE_PAXOS_ID = new Field("nr_activePaxosID", FieldType.STRING);

  public final static Field OLD_ACTIVE_PAXOS_ID = new Field("nr_oldActivePaxosID", FieldType.STRING);

  public final static Field TIME_TO_LIVE = new Field("nr_ttl", FieldType.INTEGER);

  public final static Field VALUES_MAP = new Field("nr_valuesMap", FieldType.VALUES_MAP);

  public final static Field OLD_VALUES_MAP = new Field("nr_oldValuesMap", FieldType.VALUES_MAP);

  public final static Field TOTAL_UPDATE_REQUEST = new Field("nr_totalUpdateRequest", FieldType.INTEGER);

  public final static Field TOTAL_LOOKUP_REQUEST = new Field("nr_totalLookupRequest", FieldType.INTEGER);

  /**
   * This HashMap stores all the (field,value) tuples that are read from the database for this name record.
   */
  private HashMap<Field, Object> hashMap;


  /********************************************
   * CONSTRUCTORS
   * ******************************************/

  /**
   * Creates a <code>NameRecord</code> object initialized with given fields. The record is in memory and not written to DB.
   * @param name
   * @param activeNameServers
   * @param activePaxosID
   * @param values
   * @return
   */
  public NameRecord(String name, Set<Integer> activeNameServers, String activePaxosID,
                                ValuesMap values) {
    hashMap = new HashMap<Field, Object>();
    hashMap.put(NAME,name);
    hashMap.put(ACTIVE_NAMESERVERS, activeNameServers);
    hashMap.put(PRIMARY_NAMESERVERS, HashFunction.getPrimaryReplicas(name));
    hashMap.put(ACTIVE_PAXOS_ID, activePaxosID);
    hashMap.put(OLD_ACTIVE_PAXOS_ID, name + "-1");
    hashMap.put(TIME_TO_LIVE, 0);
    hashMap.put(VALUES_MAP, values);
    hashMap.put(OLD_VALUES_MAP, new ValuesMap());
    hashMap.put(TOTAL_LOOKUP_REQUEST, 0);
    hashMap.put(TOTAL_UPDATE_REQUEST, 0);
  }

//  private static Object getObject(Field field, JSONObject jsonObject) throws JSONException{
//    if (jsonObject.has(field.getFieldName())) {
//      switch (field.type()) {
//        case INTEGER:
//          return jsonObject.getInt(field.getFieldName());
//        case STRING:
//          return jsonObject.getString(field.getFieldName());
//        case SET_INTEGER:
//          return JSONUtils.JSONArrayToSetInteger(jsonObject.getJSONArray(field.getFieldName()));
//        case LIST_STRING:
//          return JSONUtils.JSONArrayToArrayList(jsonObject.getJSONArray(field.getFieldName()));
//        case MAP:
//          return new ValuesMap(jsonObject.getJSONObject(field.getFieldName()));
//      }
//    }
//    return null;
//  }

  /**
   * Creates a <code>NameRecord</code> object by reading fields from the JSONObject.
   * @param jsonObject
   * @return
   * @throws JSONException
   */
  public NameRecord(JSONObject jsonObject) throws JSONException{

    hashMap = new HashMap<Field, Object>();
    if (jsonObject.has(NAME.getFieldName())) {
      hashMap.put(NAME, JSONUtils.getObject(NAME, jsonObject));
    }

    if (jsonObject.has(ACTIVE_NAMESERVERS.getFieldName())) {
      hashMap.put(ACTIVE_NAMESERVERS, JSONUtils.getObject(ACTIVE_NAMESERVERS, jsonObject));
    }

    if (jsonObject.has(PRIMARY_NAMESERVERS.getFieldName())) {
      hashMap.put(PRIMARY_NAMESERVERS, JSONUtils.getObject(PRIMARY_NAMESERVERS, jsonObject));
    }

    if (jsonObject.has(ACTIVE_PAXOS_ID.getFieldName())) {
      hashMap.put(ACTIVE_PAXOS_ID, JSONUtils.getObject(ACTIVE_PAXOS_ID, jsonObject));
    }

    if (jsonObject.has(OLD_ACTIVE_PAXOS_ID.getFieldName())) {
      hashMap.put(OLD_ACTIVE_PAXOS_ID, JSONUtils.getObject(OLD_ACTIVE_PAXOS_ID, jsonObject));
    }

    if (jsonObject.has(TIME_TO_LIVE.getFieldName())) {
      hashMap.put(TIME_TO_LIVE, JSONUtils.getObject(TIME_TO_LIVE, jsonObject));
    }

    if (jsonObject.has(VALUES_MAP.getFieldName())) {
      hashMap.put(VALUES_MAP, JSONUtils.getObject(VALUES_MAP, jsonObject));
    }

    if (jsonObject.has(OLD_VALUES_MAP.getFieldName())) {
      hashMap.put(VALUES_MAP, JSONUtils.getObject(VALUES_MAP, jsonObject));
    }

    if (jsonObject.has(TOTAL_LOOKUP_REQUEST.getFieldName())) {
      hashMap.put(TOTAL_LOOKUP_REQUEST, JSONUtils.getObject(TOTAL_LOOKUP_REQUEST, jsonObject));
    }

    if (jsonObject.has(TOTAL_UPDATE_REQUEST.getFieldName())) {
      hashMap.put(TOTAL_UPDATE_REQUEST, JSONUtils.getObject(TOTAL_UPDATE_REQUEST, jsonObject));
    }


//    NameRecord record = new NameRecord(new HashMap<Field, Object>());
//    return record;
  }

  /**
   * Constructor used by the initialize values read from database
   * @param allValues
   */
  public NameRecord(HashMap<Field,Object> allValues) {
    this.hashMap = allValues;
  }

  /**
   * Creates an empty name record without checking if name exists in database.
   * @param name
   */
  public NameRecord(String name) {
    hashMap = new HashMap<Field, Object>();
    hashMap.put(NAME,name);
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }

  public JSONObject toJSONObject() throws JSONException{
    JSONObject jsonObject = new JSONObject();
    for (Field f: hashMap.keySet()) {
      JSONUtils.putFieldInJsonObject(f, hashMap.get(f),jsonObject);
    }
    return jsonObject;
  }


//  /**
//   * Reads the complete record from database if fullRecord == true.
//   * if fullRecord == false, still reads the database to see if name record exists.
//   * Throws <code>RecordNotFoundException</code> if the name does not exist in database.
//   * @param name
//   * @param fullRecord
//   */
//  public NameRecord(String name, boolean fullRecord) throws RecordNotFoundException{
//
//  }


  /********************************************
   * GETTER methods for each field in name record
   * ******************************************/

  public String getName() throws FieldNotFoundException {
    if (hashMap.containsKey(NAME)) return (String) hashMap.get(NAME);
    throw new FieldNotFoundException(NAME);
  }
  public Set<Integer> getActiveNameServers() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) return (Set<Integer>)hashMap.get(ACTIVE_NAMESERVERS);
    throw new FieldNotFoundException(ACTIVE_NAMESERVERS);
  }

  public Set<Integer> getPrimaryNameservers() throws FieldNotFoundException {
    if (hashMap.containsKey(PRIMARY_NAMESERVERS)) return (Set<Integer>)hashMap.get(PRIMARY_NAMESERVERS);
    throw new FieldNotFoundException(PRIMARY_NAMESERVERS);
  }

  public  String getActivePaxosID() throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_PAXOS_ID)) return (String)hashMap.get(ACTIVE_PAXOS_ID);
    throw new FieldNotFoundException(ACTIVE_PAXOS_ID);
  }

  public String getOldActivePaxosID() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_ACTIVE_PAXOS_ID)) return (String)hashMap.get(OLD_ACTIVE_PAXOS_ID);
    throw new FieldNotFoundException(OLD_ACTIVE_PAXOS_ID);
  }

  public  int getTimeToLive() throws FieldNotFoundException {
    if (hashMap.containsKey(TIME_TO_LIVE)) return (Integer)hashMap.get(TIME_TO_LIVE);
    throw new FieldNotFoundException(TIME_TO_LIVE);
  }

  public ValuesMap getValuesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) return (ValuesMap)hashMap.get(VALUES_MAP);
    throw new FieldNotFoundException(VALUES_MAP);
  }

  public ValuesMap getOldValuesMap() throws FieldNotFoundException {
    if (hashMap.containsKey(OLD_VALUES_MAP)) return (ValuesMap)hashMap.get(OLD_VALUES_MAP);
    throw new FieldNotFoundException(OLD_VALUES_MAP);
  }

  public int getTotalLookupRequest() throws FieldNotFoundException {
    if (hashMap.containsKey(TOTAL_LOOKUP_REQUEST)) return (Integer)hashMap.get(TOTAL_LOOKUP_REQUEST);
    throw new FieldNotFoundException(TOTAL_LOOKUP_REQUEST);
  }

  public int getTotalUpdateRequest() throws FieldNotFoundException {
    if (hashMap.containsKey(TOTAL_UPDATE_REQUEST)) return (Integer)hashMap.get(TOTAL_UPDATE_REQUEST);
    throw new FieldNotFoundException(TOTAL_UPDATE_REQUEST);
  }



//  public List<String> getKey(String fieldName) throws FieldNotFoundException {
//    return (List<String>) ((Map) hashMap.get(VALUES_MAP)).get(fieldName);
//
//  }


  /********************************************
   * READ methods
   * ******************************************/

  /**
   * Checks whether the key exists in the values map for this name record.
   *
   * Call this method after reading this key from the database. If the key is not found, then name record does not exist.
   * @param key
   * @return
   * @throws FieldNotFoundException
   */
  public boolean containsKey(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      return valuesMap.containsKey(key);
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  /**
   * Returns the list of values for this key, if the key exists and the key is read from database already.
   * Throws FieldNotFoundException if (1) key has not been read from database
   * or (2) key does not exist for this name record.
   *
   * Call this method only if <code>containsKey</code> returns true, otherwise return false.
   *
   * @param key
   * @return
   * @throws FieldNotFoundException
   */
  public ArrayList<String> getKey(String key) throws FieldNotFoundException {
    if (hashMap.containsKey(VALUES_MAP)) {
      ValuesMap valuesMap = (ValuesMap) hashMap.get(VALUES_MAP);
      if (valuesMap.containsKey(key)) {
        return valuesMap.get(key);
      }
    }
    throw new FieldNotFoundException(VALUES_MAP);
  }

  public boolean containsActiveNameServer(int id) throws FieldNotFoundException {
    if (hashMap.containsKey(ACTIVE_NAMESERVERS)) {
      return ((Set<Integer>) hashMap.get(ACTIVE_NAMESERVERS)).contains(id);
    }
    throw  new FieldNotFoundException(ACTIVE_NAMESERVERS);
  }

  /**
   * ACTIVE: checks whether paxosID is current active Paxos/oldactive paxos/neither. .
   * @param paxosID
   * @return
   * @throws FieldNotFoundException
   */
  public int getPaxosStatus(String paxosID) throws FieldNotFoundException{
    String activePaxosID = getActivePaxosID();
    String oldActivePaxosID = getOldActivePaxosID();
    if (activePaxosID != null && activePaxosID.equals(paxosID)) {
      return 1; // CONSIDER TURNING THESE INTS INTO ENUMERATED VALUES!
    }
    if (oldActivePaxosID != null && oldActivePaxosID.equals(paxosID)) {
      return 2;
    }
    return 3;

  }

  /**
   *
   * @param oldPaxosID
   * @return
   * @throws FieldNotFoundException
   */
  public ValuesMap getOldValuesOnPaxosIDMatch(String oldPaxosID) throws FieldNotFoundException{
    if (oldPaxosID.equals(getOldActivePaxosID())) {
      //return oldValuesList;
      return getOldValuesMap();
    }
    return null;
  }

  /********************************************
   * WRITE methods, these methods change one or more fields in database.
   * ******************************************/


  public void incrementLookupRequest() throws FieldNotFoundException {
    ArrayList<Field> incrementFields = new ArrayList<Field>();
    incrementFields.add(TOTAL_LOOKUP_REQUEST);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);

    NameServer.recordMap.increment(getName(),incrementFields,values);
    // TODO implement batching
  }

  public void incrementUpdateRequest() throws FieldNotFoundException {
    ArrayList<Field> incrementFields = new ArrayList<Field>();
    incrementFields.add(TOTAL_UPDATE_REQUEST);

    ArrayList<Object> values = new ArrayList<Object>();
    values.add(1);

    NameServer.recordMap.increment(getName(),incrementFields,values);
  }


  public boolean updateKey(String key, ArrayList<String> newValues, ArrayList<String> oldValues,
                           UpdateOperation operation) throws FieldNotFoundException{
    ValuesMap valuesMap;
    if (operation.equals(UpdateOperation.REPLACE_ALL)) {
      valuesMap = new ValuesMap();
      hashMap.put(VALUES_MAP, valuesMap);
    } else {
      valuesMap = getValuesMap(); // this will throw an exception if field is not read.
    }
    boolean updated = UpdateOperation.updateValuesMap(valuesMap, key, newValues, oldValues, operation); //this updates the values map as well
    if (updated) {
      // commit update to database
      ArrayList<Field> updatedFields = new ArrayList<Field>();
      updatedFields.add(new Field(key,FieldType.LIST_STRING));
      ArrayList<Object> updatedValues = new ArrayList<Object>();
      updatedValues.add(valuesMap.get(key));

      NameServer.recordMap.update(getName(), NAME, null, null, VALUES_MAP, updatedFields, updatedValues);
//      valuesMap.get();
    }
    return updated;
  }

  public void handleCurrentActiveStop(String paxosID) throws FieldNotFoundException{
    String currentPaxosID = getActivePaxosID();
    ValuesMap valuesMap = getValuesMap();
    if (currentPaxosID != null && currentPaxosID.equals(paxosID)) {
      ArrayList<Field> updateFields = new ArrayList<Field>();
      updateFields.add(OLD_ACTIVE_PAXOS_ID);
      updateFields.add(ACTIVE_PAXOS_ID);
      updateFields.add(ACTIVE_NAMESERVERS);
      updateFields.add(OLD_VALUES_MAP);
      updateFields.add(VALUES_MAP);

      ArrayList<Object> updateValues = new ArrayList<Object>();
      updateValues.add(paxosID);
      updateValues.add("NULL");
      updateValues.add(new HashSet<Integer>());
      updateValues.add(valuesMap);
      updateValues.add(new ValuesMap());

      NameServer.recordMap.update(getName(),NAME,updateFields,updateValues);

      hashMap.put(OLD_ACTIVE_PAXOS_ID,paxosID);
      hashMap.put(ACTIVE_PAXOS_ID,"NULL");
      hashMap.put(ACTIVE_NAMESERVERS,new HashSet<Integer>());
      hashMap.put(OLD_VALUES_MAP, valuesMap);
      hashMap.put(VALUES_MAP, new ValuesMap());
    }

//    if (getActivePaxosID().equals(paxosID)) {
//      // current set of actives has stopped.
//      // copy all fields to "oldActive" variables
//      // initialize "active" variables to null
//      setOldActivePaxosID(paxosID);
//      setActivePaxosID(null);
//      setActiveNameservers(null);
//      setOldValuesMap(valuesMap);
//      setValuesMap(new ValuesMap());
//      if (StartNameServer.debugMode) {
//        GNS.getLogger().fine(" Updated variables after current active stopped. ");
//      }
//    } else {
//
//    }

  }

  public void handleNewActiveStart(Set<Integer> actives, String paxosID, ValuesMap currentValue)
          throws FieldNotFoundException{
    ArrayList<Field> updateFields = new ArrayList<Field>();
    updateFields.add(NameRecord.ACTIVE_NAMESERVERS);
    updateFields.add(NameRecord.ACTIVE_PAXOS_ID);
    updateFields.add(NameRecord.VALUES_MAP);

    ArrayList<Object> updateValues = new ArrayList<Object>();
    updateValues.add(actives);
    updateValues.add(paxosID);
    updateValues.add(currentValue);

    NameServer.recordMap.update(getName(),NAME,updateFields,updateValues);

    hashMap.put(ACTIVE_NAMESERVERS,actives);
    hashMap.put(ACTIVE_PAXOS_ID, paxosID);
    hashMap.put(VALUES_MAP, currentValue);

  }

  /********************************************
   * SETTER methods, these methods write to database one field in the name record.
   * ******************************************/


  public void setValuesMap(ValuesMap valuesMap) {

    throw new UnsupportedOperationException();
  }


  public void setActiveNameServers(Set<Integer> activeNameServers1) {
    throw new UnsupportedOperationException();
  }

  public void setActiveNameservers(Set<Integer> activeNameservers) {
    throw new UnsupportedOperationException();
  }

  public void setActivePaxosID(String activePaxosID) {
    throw new UnsupportedOperationException();
  }

  public void setOldActivePaxosID(String oldActivePaxosID) {
    throw new UnsupportedOperationException();
  }

  public void setTotalLookupRequest(int totalLookupRequest) {
    throw new UnsupportedOperationException();
  }

  public void setTotalUpdateRequest(int totalUpdateRequest) {
    throw new UnsupportedOperationException();
  }

  public void setOldValuesMap(ValuesMap oldValuesMap) {
    throw new UnsupportedOperationException();
  }
  
  /**
   * PLEASE DO NOT DELETE THE implements Comparable<NameRecord> BELOW. IT IS NECESSARY!!!! - Westy
   * 
   * @param d
   * @return 
   */
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


}

