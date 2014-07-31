/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NameRecordKey;
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

  public static CommandResponse lookup(String guid, String key, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message, ColumnFieldType.USER_JSON);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ValuesMap valuesMap = result.getValuesMapSansInternalFields();
      try {
        resultString = valuesMap.get(key).toString();
      } catch (JSONException e) {
        resultString = Defs.BADRESPONSE + " " + Defs.JSONPARSEERROR + " " + e;
      }
    }
    return new CommandResponse(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder());
  }

  // old style read
  public static CommandResponse lookupJSONArray(String guid, String key, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.getArray(key);
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

  public static CommandResponse lookupMultipleValues(String guid, String key, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message, ColumnFieldType.USER_JSON);
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

  public static CommandResponse lookupOne(String guid, String key, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message, ColumnFieldType.USER_JSON);
    if (result.isError()) {
      resultString = Defs.BADRESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.getArray(key);
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

  public static CommandResponse lookupOneMultipleValues(String guid, String key, String reader, String signature, String message) {

    String resultString;
    QueryResult result = Intercessor.sendQuery(guid, key, reader, signature, message, ColumnFieldType.USER_JSON);
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

  public static NSResponseCode create(String guid, String key, ResultValue value, String writer, String signature, String message) {

    return Intercessor.sendUpdateRecord(guid, key, value, null, -1, UpdateOperation.SINGLE_FIELD_CREATE, writer, signature, message);
  }

  public static CommandResponse select(String key, Object value) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.EQUALS, new NameRecordKey(key), value, null);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }

  public static CommandResponse selectWithin(String key, String value) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.WITHIN, new NameRecordKey(key), value, null);
    if (result != null) {
      return new CommandResponse(result);
    } else {
      return new CommandResponse(emptyJSONArrayString);
    }
  }

  public static CommandResponse selectNear(String key, String value, String maxDistance) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.NEAR, new NameRecordKey(key), value, maxDistance);
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
