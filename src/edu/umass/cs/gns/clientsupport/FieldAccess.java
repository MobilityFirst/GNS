/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
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
  private static final String emptyString = "";

  public static String lookup(String guid, String key, String reader, String signature, String message) {

    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message);
    if (result.isError()) {
      return Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.get(key);
      if (value != null) {
        return new JSONArray(value).toString();
      } else {
        return emptyJSONArrayString;
      }
    }
  }

  public static String lookupMultipleValues(String guid, String key, String reader, String signature, String message) {

    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message);
    if (result.isError()) {
      return Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      try {
         // pull out all the key pairs ignoring "system" (ie., non-user) fields
        return result.getValuesMapSansInternalFields().toJSONObject().toString();
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem parsing multiple value return:" + e);
      }
      return emptyJSONObjectString;
    }
  }

  public static String lookupOne(String guid, String key, String reader, String signature, String message) {

    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message);
    if (result.isError()) {
      return Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.get(key);
      if (value != null && !value.isEmpty()) {
        return (String) value.get(0);
      } else {
        return emptyString;
      }
    }
  }

  public static String lookupOneMultipleValues(String guid, String key, String reader, String signature, String message) {

    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message);
    if (result.isError()) {
      return Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      try {
        // pull out the first value of each key pair ignoring "system" (ie., non-user) fields
        return result.getValuesMapSansInternalFields().toJSONObjectFirstOnes().toString();
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem parsing multiple value return:" + e);
      }
      return emptyJSONObjectString;
    }
  }

  public static NSResponseCode update(String guid, String key, ResultValue value, ResultValue oldValue, int argument,
          UpdateOperation operation,
          String writer, String signature, String message) {

    return Intercessor.sendUpdateRecord(guid, key, value, oldValue, argument, 
            operation, writer, signature, message);
  }

  public static NSResponseCode create(String guid, String key, ResultValue value, String writer, String signature, String message) {

    return Intercessor.sendUpdateRecord(guid, key, value, null, -1, UpdateOperation.CREATE, writer, signature, message);
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
