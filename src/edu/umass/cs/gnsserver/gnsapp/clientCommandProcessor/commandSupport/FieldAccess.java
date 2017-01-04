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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.Select;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.AclCheckResult;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAuthentication;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSUpdateSupport;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.UnsupportedEncodingException;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectGroupBehavior;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.utils.Config;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.time.DateUtils;

/**
 * Provides static methods for sending and retrieve data values to and from the
 * the database which stores the fields and values.
 * Provides conversion between the database to java objects.
 *
 *
 * @author westy, arun
 */
public class FieldAccess {

  private final static Logger LOGGER = Logger.getLogger(FieldAccess.class.getName());

  private static final String EMPTY_JSON_ARRAY_STRING = new JSONArray().toString();
  private static final String EMPTY_STRING = "";

  /* false means that even single field queries will return a JSONObject response
   * with a single key and value. The client code has been modified accordingly.
   * The server-side modifications involve changes to AccountAccess to handle
   * lookupGuidLocally and lookupPrimaryGuid differently.
   */
  /**
   *
   */
  protected static final boolean SINGLE_FIELD_VALUE_ONLY = false;//true;

  /**
   * Reads the value of field in a guid.
   * Field(s) is a string the naming the field(s). Field(s) can us dot
   * notation to indicate subfields.
   *
   * @param header
   *
   * @param guid
   * @param field - mutually exclusive with fields
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return the value of a single field
   */
  public static CommandResponse lookupSingleField(InternalRequestHeader header, String guid, String field,
          String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    ResponseCode errorCode = signatureAndACLCheckForRead(header, guid, field, null,
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new CommandResponse(errorCode, GNSProtocol.BAD_RESPONSE.toString() + " " + errorCode.getProtocolCode());
    }
    ValuesMap valuesMap;
    try {
      valuesMap = NSFieldAccess.lookupJSONFieldLocally(header, guid, field, handler.getApp());
      // note: reader can also be null here
      if (!header.verifyInternal()) {
        // don't strip internal fields when doing a read for other servers
        valuesMap = valuesMap.removeInternalFields();
      }
      if (valuesMap != null) {
        /* arun: changed to not rely on JSONException. The previous code was relying
    	   on valuesMap.getString() throwing a JSONException and conveying a GNSProtocol.JSON_PARSE_ERROR.toString(), 
    	   which is incorrect in this case because it should give a FIELD_NOT_FOUND_EXCEPTION
    	   to the client.
         */
        if (valuesMap.isNull(field)) {
          return new CommandResponse(ResponseCode.FIELD_NOT_FOUND_EXCEPTION,
                  GNSProtocol.BAD_RESPONSE.toString() + " "
                  + GNSProtocol.FIELD_NOT_FOUND.toString() + " " + guid +":" + field + " ");
        } else {
          // arun: added support for SINGLE_FIELD_VALUE_ONLY flag
          return new CommandResponse(ResponseCode.NO_ERROR,
                  SINGLE_FIELD_VALUE_ONLY ? valuesMap.getString(field)
                          : valuesMap.toString());
        }
      } else {
        return new CommandResponse(ResponseCode.NO_ERROR, EMPTY_STRING);
      }

    } catch (FailedDBOperationException e) {
      return new CommandResponse(ResponseCode.DATABASE_OPERATION_ERROR, GNSProtocol.BAD_RESPONSE.toString()
              + " " + GNSProtocol.DATABASE_OPERATION_ERROR.toString() + " " + e);
    } catch (JSONException e) {
      return new CommandResponse(ResponseCode.JSON_PARSE_ERROR, GNSProtocol.BAD_RESPONSE.toString()
              + " " + GNSProtocol.JSON_PARSE_ERROR.toString() + " " + e);
    }

  }

  /**
   * Reads the value of fields in a guid.
   * Field(s) is a string the naming the field(s). Field(s) can us dot
   * notation to indicate subfields.
   *
   * @param header
   *
   * @param guid
   * @param fields - mutually exclusive with field
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return the value of a single field
   */
  public static CommandResponse lookupMultipleFields(InternalRequestHeader header, String guid,
          ArrayList<String> fields,
          String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    ResponseCode errorCode = signatureAndACLCheckForRead(header, guid, null, fields,
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new CommandResponse(errorCode, GNSProtocol.BAD_RESPONSE.toString() + " " + errorCode.getProtocolCode());
    }
    ValuesMap valuesMap;
    try {
      valuesMap = NSFieldAccess.lookupFieldsLocalNoAuth(header, guid, fields, ColumnFieldType.USER_JSON, handler);
      // note: reader can also be null here
      if (!header.verifyInternal()) {
        // don't strip internal fields when doing a read for other servers
        valuesMap = valuesMap.removeInternalFields();
      }
      return new CommandResponse(ResponseCode.NO_ERROR, valuesMap.toString()); // multiple field return
    } catch (FailedDBOperationException e) {
      return new CommandResponse(ResponseCode.DATABASE_OPERATION_ERROR, GNSProtocol.BAD_RESPONSE.toString()
              + " " + GNSProtocol.DATABASE_OPERATION_ERROR.toString() + " " + e);
    }

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
   * @param timestamp
   * @param message
   * @param handler
   * @return a command response
   */
  public static CommandResponse lookupJSONArray(InternalRequestHeader header, String guid,
          String field, String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = signatureAndACLCheckForRead(header, guid, field, null,
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new CommandResponse(errorCode, GNSProtocol.BAD_RESPONSE.toString() + " " + errorCode.getProtocolCode());
    }
    String resultString;
    ResultValue value = NSFieldAccess.lookupListFieldLocallySafe(guid, field, handler.getApp().getDB());
    if (!value.isEmpty()) {
      try {
        resultString = new JSONObject().put(field, value).toString();
      } catch (JSONException e) {
        return new CommandResponse(ResponseCode.JSON_PARSE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + ResponseCode.JSON_PARSE_ERROR);
      }
    } else {
      resultString = new JSONObject().toString();
    }
    return new CommandResponse(ResponseCode.NO_ERROR, resultString);
  }

  /**
   * Reads the value of all the fields in a guid.
   * Doesn't return internal system fields.
   *
   * @param header
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @param timestamp
   * @return a command response
   */
  public static CommandResponse lookupMultipleValues(InternalRequestHeader header, String guid,
          String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, guid,
            GNSProtocol.ENTIRE_RECORD.toString(), null,
            reader, signature, message, timestamp,
            handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new CommandResponse(errorCode, GNSProtocol.BAD_RESPONSE.toString() + " " + errorCode.getProtocolCode());
    }
    String resultString;
    ResponseCode responseCode;
    try {
      ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldLocally(header, guid, GNSProtocol.ENTIRE_RECORD.toString(), handler.getApp());
      if (valuesMap != null) {
        resultString = valuesMap.removeInternalFields().toString();
        responseCode = ResponseCode.NO_ERROR;
      } else {
        resultString = GNSProtocol.BAD_RESPONSE.toString();
        responseCode = ResponseCode.BAD_GUID_ERROR;
      }
    } catch (FailedDBOperationException e) {
      resultString = GNSProtocol.BAD_RESPONSE.toString();
      responseCode = ResponseCode.DATABASE_OPERATION_ERROR;
    }
    return new CommandResponse(responseCode, resultString);
  }

  /**
   * Returns the first value of the field in a guid in a an old-style JSONArray field value.
   *
   * @param guid
   * @param field
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a command response
   */
  public static CommandResponse lookupOne(InternalRequestHeader header, String guid, String field,
          String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = signatureAndACLCheckForRead(header, guid, field, null,
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new CommandResponse(errorCode, GNSProtocol.BAD_RESPONSE.toString() + " " + errorCode.getProtocolCode());
    }
    String resultString;
    ResultValue value = NSFieldAccess.lookupListFieldLocallySafe(guid, field, handler.getApp().getDB());
    if (!value.isEmpty()) {
      Object singleValue = value.get(0);
      if (singleValue instanceof Number) {
        resultString = singleValue.toString();
      } else {
        resultString = (String) value.get(0);
      }
    } else {
      return new CommandResponse(ResponseCode.FIELD_NOT_FOUND_ERROR,
              GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.FIELD_NOT_FOUND.toString());
    }
    return new CommandResponse(ResponseCode.NO_ERROR, resultString);
  }

  /**
   * Returns the first value of all the fields in a guid in an old-style JSONArray field value.
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param handler
   * @return a command response
   */
  public static CommandResponse lookupOneMultipleValues(InternalRequestHeader header, String guid, String reader,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, guid,
            GNSProtocol.ENTIRE_RECORD.toString(), null,
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new CommandResponse(errorCode, GNSProtocol.BAD_RESPONSE.toString() + " " + errorCode.getProtocolCode());
    }
    String resultString;
    ResponseCode responseCode;
    try {
      ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldLocally(null, guid,
              GNSProtocol.ENTIRE_RECORD.toString(), handler.getApp());
      if (valuesMap != null) {
        resultString = valuesMap.removeInternalFields().toJSONObjectFirstOnes().toString();
        responseCode = ResponseCode.NO_ERROR;
      } else {
        resultString = GNSProtocol.BAD_RESPONSE.toString();
        responseCode = ResponseCode.BAD_GUID_ERROR;
      }
    } catch (FailedDBOperationException e) {
      resultString = GNSProtocol.BAD_RESPONSE.toString();
      responseCode = ResponseCode.DATABASE_OPERATION_ERROR;
    } catch (JSONException e) {
      resultString = GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.JSON_PARSE_ERROR.toString() + " " + e.getMessage();
      responseCode = ResponseCode.JSON_PARSE_ERROR;
    }
    return new CommandResponse(responseCode, resultString);
  }

  /**
   * Updates the field with value.
   *
   * @param header
   * @param guid - the guid to update
   * @param key - the field to update
   * @param value - the new value
   * @param oldValue - the old value - only applicable for certain operations, null otherwise
   * @param argument - for operations that require an index, -1 otherwise
   * @param operation - the update operation to perform... see <code>UpdateOperation</code>
   * @param writer - the guid performing the write operation, can be the same as the guid being written. Can be null for globally
   * readable or writable fields or the secret for internal operations done without a signature.
   * @param signature - the signature of the request. Used for authentication at the server. Can be null for globally
   * readable or writable fields or the secret for internal operations done without a signature.
   * @param message - the message that was signed. Used for authentication at the server. Can be null for globally
   * readable or writable fields or the secret for internal operations done without a signature.
   * @param timestamp
   * @param handler
   * @return an NSResponseCode
   */
  public static ResponseCode update(InternalRequestHeader header, String guid, String key, String value, String oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message,
          Date timestamp,
          ClientRequestHandlerInterface handler) {
    return update(header, guid, key,
            new ResultValue(Arrays.asList(value)),
            oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
            argument,
            operation,
            writer, signature, message, timestamp, handler);
  }

  /**
   * Updates the field with value.
   *
   * @param header
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
   * @param timestamp
   * @param handler
   * @return an NSResponseCode
   */
  public static ResponseCode update(InternalRequestHeader header, String guid, String key,
          ResultValue value, ResultValue oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message,
          Date timestamp,
          ClientRequestHandlerInterface handler) {

    try {
      return NSUpdateSupport.executeUpdateLocal(header, guid, key, writer, signature, message,
              timestamp,
              operation,
              value, oldValue, argument, null, handler.getApp(), false);
    } catch (JSONException e) {
      LOGGER.log(Level.FINE, "Update threw error: {0}", e);
      return ResponseCode.JSON_PARSE_ERROR;
    } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException |
            SignatureException | IOException | InternalRequestException |
            FailedDBOperationException | RecordNotFoundException | FieldNotFoundException e) {
      LOGGER.log(Level.FINE, "Update threw error: {0}", e);
      return ResponseCode.UPDATE_ERROR;
    }
  }

  /**
   * Sends an update request to the server containing a JSON Object.
   *
   * @param header
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
  private static ResponseCode update(InternalRequestHeader header,
          String guid, JSONObject json, UpdateOperation operation,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    try {
      return NSUpdateSupport.executeUpdateLocal(header, guid, null,
              writer, signature, message, timestamp, operation,
              null, null, -1, new ValuesMap(json), handler.getApp(), false);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException |
            SignatureException | JSONException | IOException | InternalRequestException |
            FailedDBOperationException | RecordNotFoundException | FieldNotFoundException e) {
      LOGGER.log(Level.FINE, "Update threw error: {0}", e);
      return ResponseCode.UPDATE_ERROR;
    }
  }

  /**
   * Sends an update request to the server containing a JSON Object.
   *
   * @param header
   * @param guid - the guid to update
   * @param json - the JSONObject to use in the update
   * @param writer - the guid performing the write operation, can be the same as the guid being written. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param signature - the signature of the request. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param message - the message that was signed. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param timestamp
   * @param handler
   * @return an NSResponseCode
   */
  public static ResponseCode updateUserJSON(InternalRequestHeader header, String guid, JSONObject json,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.update(header, guid, new ValuesMap(json),
            UpdateOperation.USER_JSON_REPLACE,
            writer, signature, message, timestamp, handler);
  }

  /**
   * Sends an update request to the server containing a JSON Object.
   * This is a convenience method - one could use the <code>update</code> method.
   *
   * @param header
   * @param guid - the guid to update
   * @param key - the field to createField
   * @param value - the initial value of the field
   * @param writer - the guid performing the createField operation, can be the same as the guid being written. Can be null for globally
   * readable or writable fields or the secret for internal operations done without a signature.
   * @param signature - the signature of the request. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param message - the message that was signed. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param timestamp
   * @param handler
   * @return a {@link ResponseCode}
   */
  public static ResponseCode createField(InternalRequestHeader header, String guid, String key, ResultValue value,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    return update(header, guid, key, value, null, -1,
            UpdateOperation.SINGLE_FIELD_CREATE, writer, signature, message,
            timestamp, handler);
  }

  /**
   * Deletes the field from the guid.
   *
   * @param header
   * @param guid - the guid to update
   * @param key - the field to createField
   * @param writer - the guid performing the delete operation, can be the same as the guid being written.
   * Can be null for globally readable or writable fields or the secret for internal operations done without a signature.
   * @param signature - the signature of the request. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param message - the message that was signed. Used for authentication at the server. Can be null for globally
   * readable or writable fields or for internal operations done without a signature.
   * @param timestamp
   * @param handler
   * @return a {@link ResponseCode}
   */
  public static ResponseCode deleteField(InternalRequestHeader header, String guid, String key,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    return update(header, guid, key,
            "", null, -1, // these are ignored anyway
            UpdateOperation.SINGLE_FIELD_REMOVE_FIELD, writer, signature, message,
            timestamp, handler);
  }

  private static JSONArray executeSelect(InternalRequestHeader header, SelectOperation operation, String key, Object value, Object otherValue, GNSApp app)
          throws FailedDBOperationException, JSONException, UnknownHostException, InternalRequestException {
    SelectRequestPacket<String> packet = new SelectRequestPacket<>(-1, operation,
            SelectGroupBehavior.NONE, key, value, otherValue);
    return executeSelectHelper(header, packet, app);
  }

  private static JSONArray executeSelectHelper(InternalRequestHeader header, SelectRequestPacket<String> packet, GNSApp app)
          throws FailedDBOperationException, JSONException, UnknownHostException, InternalRequestException {
    SelectResponsePacket<String> responsePacket = Select.handleSelectRequestFromClient(header, packet, app);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(responsePacket.getResponseCode())) {
      return responsePacket.getGuids();
    } else {
      return null;
    }
  }

  /**
   * Sends a select request to the server to retrieve all the guids matching the request.
   *
   * @param key - the key to match
   * @param value - the value to match
   * @param handler
   * @return a command response
 * @throws InternalRequestException 
   */
  public static CommandResponse select(InternalRequestHeader header, String key, Object value, ClientRequestHandlerInterface handler) throws InternalRequestException {
    JSONArray result;
    try {
      //if (Select.useLocalSelect()) {
        result = executeSelect(header, SelectOperation.EQUALS, key, value, null, handler.getApp());
//      } else {
//        result = handler.getRemoteQuery().sendSelect(SelectOperation.EQUALS, key, value, null);
//      }
      if (result != null) {
        return new CommandResponse(ResponseCode.NO_ERROR, result.toString());
      }
    } catch (IOException | JSONException | FailedDBOperationException e) {
    //} catch (ClientException | IOException | JSONException | FailedDBOperationException e) {
    }
    return new CommandResponse(ResponseCode.NO_ERROR, EMPTY_JSON_ARRAY_STRING);
  }

  /**
   * Sends a select request to the server to retrieve all the guids within an area specified by a bounding box.
   *
   * @param key - the field to match - should be a location field
   * @param value - a bounding box
   * @param handler
   * @return a command response
 * @throws InternalRequestException 
   */
  public static CommandResponse selectWithin(InternalRequestHeader header, String key, String value,
          ClientRequestHandlerInterface handler) throws InternalRequestException {
    JSONArray result;
    try {
      //if (Select.useLocalSelect()) {
        result = executeSelect(header, SelectOperation.WITHIN, key, value, null, handler.getApp());
//      } else {
//        result = handler.getRemoteQuery().sendSelect(SelectOperation.WITHIN, key, value, null);
//      }
      if (result != null) {
        return new CommandResponse(ResponseCode.NO_ERROR, result.toString());
      }
    } catch (IOException | JSONException | FailedDBOperationException e) {
    //} catch (ClientException | IOException | JSONException | FailedDBOperationException e) {
    }
    return new CommandResponse(ResponseCode.NO_ERROR, EMPTY_JSON_ARRAY_STRING);

  }

  /**
   * Sends a select request to the server to retrieve all the guids within maxDistance of value.
   *
   * @param key - the field to match - should be a location field
   * @param value - the position
   * @param maxDistance - the maximum distance from position
   * @param handler
   * @return a command response
 * @throws InternalRequestException 
   */
  public static CommandResponse selectNear(InternalRequestHeader header, String key, String value, String maxDistance,
          ClientRequestHandlerInterface handler) throws InternalRequestException {
    JSONArray result;
    try {
      //if (Select.useLocalSelect()) {
        result = executeSelect(header, SelectOperation.NEAR, key, value, maxDistance, handler.getApp());
//      } else {
//        result = handler.getRemoteQuery().sendSelect(SelectOperation.NEAR, key, value, maxDistance);
//      }
      if (result != null) {
        return new CommandResponse(ResponseCode.NO_ERROR, result.toString());
      }
    } catch (IOException | JSONException | FailedDBOperationException e) {
    //} catch (ClientException | IOException | JSONException | FailedDBOperationException e) {
    }
    return new CommandResponse(ResponseCode.NO_ERROR, EMPTY_JSON_ARRAY_STRING);
  }

  /**
   * Sends a select request to the server to retrieve all the guid matching the query.
   *
   * @param query
   * @param handler
   * @return a command response
 * @throws InternalRequestException 
   */
  public static CommandResponse selectQuery(InternalRequestHeader header, String query, ClientRequestHandlerInterface handler) throws InternalRequestException {
    JSONArray result;
    try {
      //if (Select.useLocalSelect()) {
        SelectRequestPacket<String> packet = SelectRequestPacket.MakeQueryRequest(-1, query);
        result = executeSelectHelper(header, packet, handler.getApp());
//      } else {
//        result = handler.getRemoteQuery().sendSelectQuery(query);
//      }
      if (result != null) {
        return new CommandResponse(ResponseCode.NO_ERROR, result.toString());
      }
    } catch (IOException | JSONException | FailedDBOperationException e) {
    //} catch (ClientException | IOException | JSONException | FailedDBOperationException e) {
    }
    return new CommandResponse(ResponseCode.NO_ERROR, EMPTY_JSON_ARRAY_STRING);
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
 * @throws InternalRequestException 
   */
  public static CommandResponse selectGroupSetupQuery(InternalRequestHeader header, String accountGuid, String query, String publicKey,
          int interval,
          ClientRequestHandlerInterface handler) throws InternalRequestException {
    String guid = SharedGuidUtils.createGuidStringFromBase64PublicKey(publicKey);
    //String guid = SharedGuidUtils.createGuidStringFromPublicKey(Base64.decode(publicKey));
    // Check to see if the guid doesn't exists and if so createField it...
    if (AccountAccess.lookupGuidInfoAnywhere(header, guid, handler) == null) {
      // This code is similar to the code in AddGuid command except that we're not checking signatures... yet.
      // FIXME: This should probably include authentication
      GuidInfo accountGuidInfo;
      if ((accountGuidInfo = AccountAccess.lookupGuidInfoAnywhere(header, accountGuid, handler)) == null) {
        return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString()
                + " " + GNSProtocol.BAD_GUID.toString() + " " + accountGuid);
      }
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(header, accountGuid, handler);
      if (accountInfo == null) {
        return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString()
                + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + accountGuid);
      }
      if (!accountInfo.isVerified()) {
        return new CommandResponse(ResponseCode.VERIFICATION_ERROR, GNSProtocol.BAD_RESPONSE.toString()
                + " " + GNSProtocol.VERIFICATION_ERROR.toString() + " Account not verified");
      } else if (accountInfo.getGuids().size() > Config.getGlobalInt(GNSConfig.GNSC.ACCOUNT_GUID_MAX_SUBGUIDS)) {
        return new CommandResponse(ResponseCode.TOO_MANY_GUIDS_EXCEPTION, GNSProtocol.BAD_RESPONSE.toString()
                + " " + GNSProtocol.TOO_MANY_GUIDS.toString());
      } else {
        // The alias (HRN) of the new guid is a hash of the query.
        String name = Base64.encodeToString(ShaOneHashFunction.getInstance().hash(query), false);
        CommandResponse groupGuidCreateresult = AccountAccess.addGuid(header, accountInfo, accountGuidInfo,
                name, guid, publicKey, handler);
        // If there was a problem adding return that error response.
        if (!groupGuidCreateresult.getExceptionOrErrorCode().isOKResult()) {
          return groupGuidCreateresult;
        }
      }
    }
    JSONArray result;

    try {
      //if (Select.useLocalSelect()) {
        SelectRequestPacket<String> packet = SelectRequestPacket.MakeGroupSetupRequest(-1,
                query, guid, interval);
        result = executeSelectHelper(header, packet, handler.getApp());
//      } else {
//        result = handler.getRemoteQuery().sendGroupGuidSetupSelectQuery(query, guid, interval);
//      }
      if (result != null) {
        return new CommandResponse(ResponseCode.NO_ERROR, result.toString());
      }
    } catch (IOException | JSONException | FailedDBOperationException e) {
    //} catch (ClientException | IOException | FailedDBOperationException | JSONException e) {
    }
    return new CommandResponse(ResponseCode.NO_ERROR, EMPTY_JSON_ARRAY_STRING);
  }

  /**
   * Sends a select request to the server to retrieve the members of a context aware group guid.
   *
   * @param guid - the guid (which should have been previously initialized using <code>selectGroupSetupQuery</code>
   * @param handler
   * @return a command response
 * @throws InternalRequestException 
   */
  public static CommandResponse selectGroupLookupQuery(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler) throws InternalRequestException {
    JSONArray result;
    try {
      //if (Select.useLocalSelect()) {
        SelectRequestPacket<String> packet = SelectRequestPacket.MakeGroupLookupRequest(-1, guid);
        result = executeSelectHelper(header, packet, handler.getApp());
//      } else {
//        result = handler.getRemoteQuery().sendGroupGuidLookupSelectQuery(guid);
//      }
      if (result != null) {
        return new CommandResponse(ResponseCode.NO_ERROR, result.toString());
      }
    } catch (IOException | JSONException | FailedDBOperationException e) {
    //} catch (ClientException | IOException | FailedDBOperationException | JSONException e) {
    }
    return new CommandResponse(ResponseCode.NO_ERROR, EMPTY_JSON_ARRAY_STRING);
  }

  /**
   *
   * @param guid
   * @param field
   * @param fields
   * @param reader
   * @param signature
   * @param message
   * @param timestamp
   * @param app
   * @return the ResponseCode
   */
  public static ResponseCode signatureAndACLCheckForRead(String guid,
          String field, List<String> fields,
          String reader, String signature, String message,
          Date timestamp,
          GNSApplicationInterface<String> app) {
	  // should never come here anymore
	  assert(false);
	  return signatureAndACLCheckForRead(null, guid, field, fields, reader, signature, message, timestamp, app);
  }
  
  public static ResponseCode signatureAndACLCheckForRead(InternalRequestHeader header, String guid,
          String field, List<String> fields,
          String reader, String signature, String message,
          Date timestamp,
          GNSApplicationInterface<String> app) {
    ResponseCode errorCode = ResponseCode.NO_ERROR;
    LOGGER.log(Level.FINE,
            "signatureAndACLCheckForRead guid: {0} field: {1} reader: {2}",
            new Object[]{guid, field, reader});
    try {
    	assert(header!=null);
    	
      // if reader is the internal secret this means that this is an internal
      // request that doesn't need to be authenticated
      // note: reader can also be null here
      if (!header.verifyInternal()
              && (field != null || fields != null)) {
        errorCode = NSAuthentication.signatureAndACLCheck(header, guid, field, fields, reader,
                signature, message, MetaDataTypeName.READ_WHITELIST, app);
      } else {
    	  LOGGER.log(Level.FINE,
  	            "reader={0}; internal={1} field={2}; fields={3};",
  	            new Object[]{reader, header.verifyInternal(), field, fields});
    	  
    	  // internal commands don't need even ACL checks
    	  if (header.verifyInternal() && (GNSProtocol.INTERNAL_QUERIER.toString().equals(reader)))
    		  return ResponseCode.NO_ERROR;
          
    	  if (field != null) {
    		  errorCode = NSAuthentication.aclCheck(header, guid, field,
    				  header.getQueryingGUID(),
    				  MetaDataTypeName.READ_WHITELIST, app)
    				  .getResponseCode();
    	  } else if (fields != null) {
    		  for (String aField : fields) {
    			  AclCheckResult aclResult = NSAuthentication
    					  .aclCheck(header, guid, aField,
    							  header.getQueryingGUID(),
    							  MetaDataTypeName.READ_WHITELIST,
    							  app);
    			  if (aclResult.getResponseCode()
    					  .isExceptionOrError()) {
    				  errorCode = aclResult.getResponseCode();
    			  }
    		  }
    	  }
      }
      // Check for stale commands.
      if (timestamp != null) {
        if (timestamp.before(DateUtils.addMinutes(new Date(),
                -Config.getGlobalInt(GNSConfig.GNSC.STALE_COMMAND_INTERVAL_IN_MINUTES)))) {
          errorCode = ResponseCode.STALE_COMMAND_VALUE;
        }
      }
    } catch (InvalidKeyException | InvalidKeySpecException | SignatureException | NoSuchAlgorithmException | FailedDBOperationException | UnsupportedEncodingException e) {
      errorCode = ResponseCode.SIGNATURE_ERROR;
    }
    return errorCode;
  }

}
