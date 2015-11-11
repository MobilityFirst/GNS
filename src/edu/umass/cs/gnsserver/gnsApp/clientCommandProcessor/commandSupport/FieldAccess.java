/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GnsProtocol;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.QueryResult;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess.lookupGuidInfo;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import java.util.ArrayList;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Provides static methods for sending and retrieve data values to and from the
 * the database which stores the fields and values.
 * Provides conversion between the database to java objects.
 *
 *
 * @author westy
 */
public class FieldAccess {

  private static final String emptyJSONObjectString = new JSONObject().toString();
  private static final String emptyJSONArrayString = new JSONArray().toString();
  private static final String emptyString = "";

  /**
   * Returns true if the field is specified using dot notation.
   *
   * @param field
   * @return true if the field is specified using dot notation
   */
  public static boolean isKeyDotNotation(String field) {
    return field.indexOf('.') != -1;
  }

  /**
   * Returns true if the field doesn't use dot notation or is the all-fields indicator.
   *
   * @param field
   * @return true if the field doesn't use dot notation or is the all-fields indicator
   */
  public static boolean isKeyAllFieldsOrTopLevel(String field) {
    return GnsProtocol.ALL_FIELDS.equals(field) || !isKeyDotNotation(field);
  }

  /**
   * Reads the value of field or fields in a guid.
   * Field(s) is a string the naming the field(s). Field(s) can us dot
   * notation to indicate subfields.
   *
   * @param guid
   * @param field - mutually exclusive with fields
   * @param fields - mutually exclusive with field
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @return the value of a single field
   */
  public static CommandResponse<String> lookup(String guid, String field, ArrayList<String> fields, String reader, String signature,
          String message, ClientRequestHandlerInterface handler) {
    long startTime = System.currentTimeMillis();
    String resultString;
    QueryResult<String> result;
    if (field != null) {
      result = handler.getIntercessor().sendSingleFieldQuery(guid, field, reader, signature, message, ColumnFieldType.USER_JSON);
    } else {
      result = handler.getIntercessor().sendMultiFieldQuery(guid, fields, reader, signature, message, ColumnFieldType.USER_JSON);
    }
    if (result.isError()) {
      resultString = GnsProtocol.BAD_RESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ValuesMap valuesMap = result.getValuesMapSansInternalFields();
      try {
        if (field != null) {
          resultString = valuesMap.get(field).toString();
        } else {
          resultString = valuesMap.toString();
        }
      } catch (JSONException e) {
        resultString = GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.JSON_PARSE_ERROR + " " + e;
      }
    }
    DelayProfiler.updateDelay("FieldAccessreadLookup", startTime);
    return new CommandResponse<String>(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder()
            //,result.getLookupTime()
    );
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
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> lookupJSONArray(String guid, String field, String reader, String signature,
          String message,
          ClientRequestHandlerInterface handler) {

    String resultString;
    // Note the use of ColumnFieldType.LIST_STRING in the sendSingleFieldQuery call which implies old data format.
    QueryResult<String> result = handler.getIntercessor().sendSingleFieldQuery(guid, field, reader, signature, message, 
            ColumnFieldType.LIST_STRING);
    if (result.isError()) {
      resultString = GnsProtocol.BAD_RESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.getArray(field);
      if (value != null) {
        resultString = new JSONArray(value).toString();
      } else {
        resultString = emptyJSONArrayString;
      }
    }
    return new CommandResponse<String>(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder()
            //,result.getLookupTime()
    );
  }

  /**
   * Reads the value of all the fields in a guid.
   * Doesn't return internal system fields.
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> lookupMultipleValues(String guid, String reader, String signature, String message,
          ClientRequestHandlerInterface handler) {

    String resultString;
    QueryResult<String> result = handler.getIntercessor().sendSingleFieldQuery(guid, GnsProtocol.ALL_FIELDS, reader, signature, message, ColumnFieldType.USER_JSON);
    if (result.isError()) {
      resultString = GnsProtocol.BAD_RESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      // pull out all the key pairs ignoring "system" (ie., non-user) fields
      resultString = result.getValuesMapSansInternalFields().toString();
    }
    return new CommandResponse<String>(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder()
            //,result.getLookupTime()
    );
  }

  /**
   * Returns the first value of the field in a guid in a an old-style JSONArray field value.
   *
   * @param guid
   * @param field
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> lookupOne(String guid, String field, String reader, String signature, String message,
          ClientRequestHandlerInterface handler) {

    String resultString;
    QueryResult<String> result = handler.getIntercessor().sendSingleFieldQuery(guid, field, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (result.isError()) {
      resultString = GnsProtocol.BAD_RESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      ResultValue value = result.getArray(field);
      if (value != null && !value.isEmpty()) {
        // Ideally we could return Strings or Numbers here, but 
        // this is the "older" return type and we already support 
        // Strings or Numbers via the new JSONObject return type.
        // So for now here we're just
        // going to punt if we see a number and make it into a string.
        Object singleValue = value.get(0);
        if (singleValue instanceof Number) {
          resultString = ((Number) singleValue).toString();
        } else {
          resultString = (String) value.get(0);
        }
      } else {
        resultString = emptyString;
      }
    }
    return new CommandResponse<String>(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder()
            //,result.getLookupTime()
    );
  }

  /**
   * Returns the first value of all the fields in a guid in an old-style JSONArray field value.
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> lookupOneMultipleValues(String guid, String reader, String signature, String message,
          ClientRequestHandlerInterface handler) {

    String resultString;
    QueryResult<String> result = handler.getIntercessor().sendSingleFieldQuery(guid, GnsProtocol.ALL_FIELDS, reader, signature, message, ColumnFieldType.USER_JSON);
    if (result.isError()) {
      resultString = GnsProtocol.BAD_RESPONSE + " " + result.getErrorCode().getProtocolCode();
    } else {
      try {
        // pull out the first value of each key pair ignoring "system" (ie., non-user) fields
        resultString = result.getValuesMapSansInternalFields().toJSONObjectFirstOnes().toString();
      } catch (JSONException e) {
        GNS.getLogger().severe("Problem parsing multiple value return:" + e);
      }
      resultString = emptyJSONObjectString;
    }
    return new CommandResponse<String>(resultString,
            result.getErrorCode(),
            result.getRoundTripTime(),
            result.getResponder()
            //,result.getLookupTime()
    );
  }

  /**
   * Sends an update request to the server.
   *
   * @param guid - the guid to update
   * @param key - the field to update
   * @param value - the new value
   * @param oldValue - the old value - only applicable for certain operations, null otherwise
   * @param argument - for operations that require an index, -1 otherwise
   * @param operation - the update operation to perform... see <code>UpdateOperation</code>
   * @param writer - the guid performing the write operation, can be the same as the guid being written. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param signature - the signature of the request. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param message - the message that was signed. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param handler
   * @return an NSResponseCode
   */
  public static NSResponseCode update(String guid, String key, ResultValue value, ResultValue oldValue, int argument,
          UpdateOperation operation,
          String writer, String signature, String message, ClientRequestHandlerInterface handler) {

    return handler.getIntercessor().sendUpdateRecord(guid, key, value, oldValue, argument,
            operation, writer, signature, message);
  }

  /**
   * Sends an update request to the server containing a JSON Object.
   *
   * @param guid - the guid to update
   * @param json - the JSONObject to use in the update
   * @param operation - the update operation to perform... see <code>UpdateOperation</code>
   * @param writer - the guid performing the write operation, can be the same as the guid being written. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param signature - the signature of the request. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param message - the message that was signed. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param handler
   * @return an NSResponseCode
   */
  public static NSResponseCode update(String guid, JSONObject json, UpdateOperation operation,
          String writer, String signature, String message, ClientRequestHandlerInterface handler) {
    return handler.getIntercessor().sendUpdateUserJSON(guid, new ValuesMap(json), operation, writer, signature, message);
  }

  /**
   * Sends an update request to the server containing a JSON Object.
   * This is a convenience method - one could use the <code>update</code> method.
   *
   * @param guid - the guid to update
   * @param key - the field to create
   * @param value - the initial value of the field
   * @param writer - the guid performing the create operation, can be the same as the guid being written. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param signature - the signature of the request. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param message - the message that was signed. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param handler
   * @return a {@link NSResponseCode}
   */
  public static NSResponseCode create(String guid, String key, ResultValue value, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {
    return handler.getIntercessor().sendUpdateRecord(guid, key, value, null, -1, 
            UpdateOperation.SINGLE_FIELD_CREATE, writer, signature, message);
  }

  /**
   * Sends a select request to the server to retrieve all the guids matching the request.
   *
   * @param key - the key to match
   * @param value - the value to match
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> select(String key, Object value, ClientRequestHandlerInterface handler) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.EQUALS, key, value, null, handler);
    if (result != null) {
      return new CommandResponse<String>(result);
    } else {
      return new CommandResponse<String>(emptyJSONArrayString);
    }
  }

  /**
   * Sends a select request to the server to retrieve all the guids within an area specified by a bounding box.
   *
   * @param key - the field to match - should be a location field
   * @param value - a bounding box
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> selectWithin(String key, String value, ClientRequestHandlerInterface handler) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.WITHIN, key, value, null, handler);
    if (result != null) {
      return new CommandResponse<String>(result);
    } else {
      return new CommandResponse<String>(emptyJSONArrayString);
    }
  }

  /**
   * Sends a select request to the server to retrieve all the guids within maxDistance of value.
   *
   * @param key - the field to match - should be a location field
   * @param value - the position
   * @param maxDistance - the maximum distance from position
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> selectNear(String key, String value, String maxDistance, ClientRequestHandlerInterface handler) {
    String result = SelectHandler.sendSelectRequest(SelectRequestPacket.SelectOperation.NEAR, key, value, maxDistance, handler);
    if (result != null) {
      return new CommandResponse<String>(result);
    } else {
      return new CommandResponse<String>(emptyJSONArrayString);
    }
  }

  /**
   * Sends a select request to the server to retrieve all the guid matching the query.
   *
   * @param query
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> selectQuery(String query, ClientRequestHandlerInterface handler) {
    String result = SelectHandler.sendSelectQuery(query, handler);
    if (result != null) {
      return new CommandResponse<String>(result);
    } else {
      return new CommandResponse<String>(emptyJSONArrayString);
    }
  }

  /**
   * Sends a select request to the server to setup a context aware group guid and retrieve all the guids matching the query.
   *
   * @param accountGuid
   * @param query
   * @param publicKey
   * @param interval - the refresh interval (queries made more quickly than this will get a cached value)
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> selectGroupSetupQuery(String accountGuid, String query, String publicKey, 
          int interval,
          ClientRequestHandlerInterface handler) {
    String guid = ClientUtils.createGuidStringFromPublicKey(Base64.decode(publicKey));
    // Check to see if the guid doesn't exists and if so create it...
    if (lookupGuidInfo(guid, handler) == null) {
      // This code is similar to the code in AddGuid command except that we're not checking signatures... yet.
      // FIXME: This should probably include authentication
      GuidInfo accountGuidInfo;
      if ((accountGuidInfo = AccountAccess.lookupGuidInfo(accountGuid, handler)) == null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_GUID + " " + accountGuid);
      }
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(accountGuid, handler);
      if (accountInfo == null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ACCOUNT + " " + accountGuid);
      }
      if (!accountInfo.isVerified()) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + VERIFICATION_ERROR + " Account not verified");
      } else if (accountInfo.getGuids().size() > GNS.MAXGUIDS) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + TOO_MANY_GUIDS);
      } else {
        // The alias (HRN) of the new guid is a hash of the query.
        String name = Base64.encodeToString(SHA1HashFunction.getInstance().hash(query.getBytes()), false);
        CommandResponse<String> groupGuidCreateresult = AccountAccess.addGuid(accountInfo, accountGuidInfo, 
                name, guid, publicKey, handler);
        if (!groupGuidCreateresult.getReturnValue().equals(OK_RESPONSE)) {
          return groupGuidCreateresult;
        }
      }
    }
    // We either found or created the guid above so now we set up the actual query structure.
    String result = SelectHandler.sendGroupGuidSetupSelectQuery(query, guid, interval, handler);
    if (result != null) {
      return new CommandResponse<String>(result);
    } else {
      return new CommandResponse<String>(emptyJSONArrayString);
    }
  }

  /**
   * Sends a select request to the server to retrieve the members of a context aware group guid.
   *
   * @param guid - the guid (which should have been previously initialized using <code>selectGroupSetupQuery</code>
   * @param handler
   * @return a command response
   */
  public static CommandResponse<String> selectGroupLookupQuery(String guid, ClientRequestHandlerInterface handler) {
    String result = SelectHandler.sendGroupGuidLookupSelectQuery(guid, handler);
    if (result != null) {
      return new CommandResponse<String>(result);
    } else {
      return new CommandResponse<String>(emptyJSONArrayString);
    }
  }

  /**
   *
   */
  public static String Version = "$Revision$";
}
