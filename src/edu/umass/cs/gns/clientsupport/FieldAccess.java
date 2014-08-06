/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.util.ValuesMap;
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

  public static boolean isKeyDotNotation(String key) {
    return key.indexOf('.') != -1;
  }

  public static boolean isKeyAllFieldsOrTopLevel(String key) {
    return Defs.ALLFIELDS.equals(key) || !isKeyDotNotation(key);
  }

  /**
   * Reads the value of field in a guid.
   * Field is a string the naming the field. Field can us dot 
   * notation to indicate subfields.
   * 
   * @param guid
   * @param field
   * @param reader
   * @param signature
   * @param message
   * @return the value of a single field
   */
  public static CommandResponse lookup(String guid, String field, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, field, reader, signature, message, ColumnFieldType.USER_JSON);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ValuesMap valuesMap = result.getValuesMapSansInternalFields();
      try {
        resultString = valuesMap.get(field).toString();
      } catch (JSONException e) {
        resultString = Defs.BADRESPONSE + " " + Defs.JSONPARSEERROR + " " + e;
      }
    }
    return new CommandResponse(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder());
  }

  /**
   * Supports reading of the old style data formatted as a JSONArray of strings.
   * Much of the internal system data is still stored in this format.
   * The new format also supports JSONArrays as part of the
   * whole "guid data is a JSONObject" format so we might be
   * transitioning away from this altogether at some point.
   *
   * @param guid
   * @param field
   * @param reader
   * @param signature
   * @param message
   * @return
   */
  public static CommandResponse lookupJSONArray(String guid, String field, String reader, String signature, String message) {

    String resultString;
    // Note the use of ColumnFieldType.LIST_STRING in the sendQuery call which implies old data format.
    QueryResult result = Intercessor.sendQuery(guid, field, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.getArray(field);
      if (value != null) {
        resultString = new JSONArray(value).toString();
      } else {
        resultString = emptyJSONArrayString;
      }
    }
    return new CommandResponse(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder());
  }

  /**
   * Reads the value of all the fields in a guid.
   * Doesn't return internal system fields.
   * 
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @return 
   */
  public static CommandResponse lookupMultipleValues(String guid, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, Defs.ALLFIELDS, reader, signature, message, ColumnFieldType.USER_JSON);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      // pull out all the key pairs ignoring "system" (ie., non-user) fields
      resultString = result.getValuesMapSansInternalFields().toString();
    }
    return new CommandResponse(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder());
  }

  /**
   * Returns the first value of the field in a guid in a an old-style JSONArray field value.
   * 
   * @param guid
   * @param field
   * @param reader
   * @param signature
   * @param message
   * @return 
   */
  public static CommandResponse lookupOne(String guid, String field, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, field, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.getArray(field);
      if (value != null && !value.isEmpty()) {
        resultString = (String) value.get(0);
      } else {
        resultString = emptyString;
      }
    }
    return new CommandResponse(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder());
  }

  /**
   * Returns the first value of all the fields in a guid in an old-style JSONArray field value.
   * 
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @return 
   */
  public static CommandResponse lookupOneMultipleValues(String guid, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, Defs.ALLFIELDS, reader, signature, message, ColumnFieldType.USER_JSON);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      try {
        // pull out the first value of each key pair ignoring "system" (ie., non-user) fields
        resultString = result.getValuesMapSansInternalFields().toJSONObjectFirstOnes().toString();
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem parsing multiple value return:" + e);
      }
      resultString = emptyJSONObjectString;
    }
    return new CommandResponse(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder());
  }

  public static NSResponseCode update(String guid, String key, ResultValue value, ResultValue oldValue, int argument,
          UpdateOperation operation,
          String writer, String signature, String message) {

    return Intercessor.sendUpdateRecord(guid, key, value, oldValue, argument,
            operation, writer, signature, message);
  }
  
  public static NSResponseCode update(String guid, JSONObject json, UpdateOperation operation,
          String writer, String signature, String message) {
    return Intercessor.sendUpdateUserJSON(guid, new ValuesMap(json), operation, writer, signature, message);
  }

  public static NSResponseCode create(String guid, String key, ResultValue value, String writer, String signature, String message) {

    return Intercessor.sendUpdateRecord(guid, key, value, null, -1, UpdateOperation.SINGLE_FIELD_CREATE, writer, signature, message);
  }

  public static CommandResponse select(String key, Object value) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.EQUALS, key, value, null);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }

  public static CommandResponse selectWithin(String key, String value) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.WITHIN, key, value, null);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }

  public static CommandResponse selectNear(String key, String value, String maxDistance) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.NEAR, key, value, maxDistance);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }

  public static CommandResponse selectQuery(String query) {
    String result = SelectHandler.sendSelectQuery(query);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }

  public static CommandResponse selectGroupSetupQuery(String query, String guid) {
    String result = SelectHandler.sendGroupGuidSetupSelectQuery(query, guid);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }

  public static CommandResponse selectGroupLookupQuery(String guid) {
    String result = SelectHandler.sendGroupGuidLookupSelectQuery(guid);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }
  public static String Version = "$Revision$";
}
