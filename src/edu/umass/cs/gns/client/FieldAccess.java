/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nameserver.ValuesMap;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A frontend to the the database which stores the fields and values.
 * Provides conversion between the database to java objects.
 *
 *
 * @author westy
 */
public class FieldAccess {
  
  private static final String emptyJSONObjectString = new JSONObject().toString();
  private static final String emptyJSONArrayString = new JSONArray().toString();

  public static String lookup(String guid, String key, String reader, String signature, String message) {
    
    ResultValue result = Intercessor.sendQuery(guid, key, reader, signature, message);
    if (result != null) {
      return new JSONArray(result).toString();
    } else {
      return emptyJSONArrayString;
    }
  }

  public static String lookupMultipleValues(String guid, String key, String reader, String signature, String message) {
    
    ValuesMap result = Intercessor.sendMultipleReturnValueQuery(guid, key, true, reader, signature, message);
    try {
      if (result != null) {
        return result.toJSONObject().toString();
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing multiple value return:" + e);
    }
    return emptyJSONObjectString;
  }

  public static String lookupOne(String guid, String key, String reader, String signature, String message) {
    
    ResultValue result = Intercessor.sendQuery(guid, key, reader, signature, message);
    if (result != null && !result.isEmpty()) {
      return (String) result.get(0);
    } else {
      return new String();
    }
  }

  public static String lookupOneMultipleValues(String guid, String key, String reader, String signature, String message) {
    
    ValuesMap result = Intercessor.sendMultipleReturnValueQuery(guid, key, true, reader, signature, message);
    try {
      if (result != null) {
        // Pull the first value out of each array
        return result.toJSONObjectFirstOnes().toString();
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem parsing multiple value return:" + e);
    }
    return emptyJSONObjectString;
  }

  public static boolean update(String guid, String key, ResultValue value, ResultValue oldValue, UpdateOperation operation) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, key, value, oldValue, operation);
  }

  public static boolean create(String guid, String key, ResultValue value) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, key, value, null, UpdateOperation.CREATE);
  }

  public static String select(String key, Object value) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.EQUALS, new NameRecordKey(key), value, null);
    if (result != null) {
      return result;
    } else {
      return emptyJSONArrayString;
    }
  }

  public static String selectWithin(String key, String value) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.WITHIN, new NameRecordKey(key), value, null);
    if (result != null) {
      return result;
    } else {
      return emptyJSONArrayString;
    }
  }

  public static String selectNear(String key, String value, String maxDistance) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.NEAR, new NameRecordKey(key), value, maxDistance);
    if (result != null) {
      return result;
    } else {
      return emptyJSONArrayString;
    }
  }
  
  public static String selectQuery(String query) {
    String result = SelectHandler.sendSelectQuery(query);
    if (result != null) {
      return result;
    } else {
      return emptyJSONArrayString;
    }
  }
  public static String Version = "$Revision$";
}
