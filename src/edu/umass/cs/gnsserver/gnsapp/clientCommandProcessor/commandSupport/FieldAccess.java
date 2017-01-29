
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.GNSApp;
import edu.umass.cs.gnsserver.gnsapp.Select;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.AclCheckResult;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAuthentication;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSUpdateSupport;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectGroupBehavior;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.Config;
import org.apache.commons.lang3.time.DateUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


public class FieldAccess {

  private final static Logger LOGGER = Logger.getLogger(FieldAccess.class.getName());

  private static final String EMPTY_JSON_ARRAY_STRING = new JSONArray().toString();
  private static final String EMPTY_STRING = "";



  protected static final boolean SINGLE_FIELD_VALUE_ONLY = false;//true;


  public static CommandResponse lookupSingleField(InternalRequestHeader header, CommandPacket commandPacket, 
          String guid, String field,
          String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    ResponseCode errorCode = signatureAndACLCheckForRead(header, commandPacket, guid, field, 
            null, // fields
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

        if (valuesMap.isNull(field)) {
          return new CommandResponse(ResponseCode.FIELD_NOT_FOUND_EXCEPTION,
                  GNSProtocol.BAD_RESPONSE.toString() + " "
                  + GNSProtocol.FIELD_NOT_FOUND.toString() + " " + guid + ":" + field + " ");
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


  public static CommandResponse lookupMultipleFields(InternalRequestHeader header, CommandPacket commandPacket, 
          String guid, ArrayList<String> fields,
          String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    ResponseCode errorCode = signatureAndACLCheckForRead(header, commandPacket, guid, 
            null, //field
            fields,
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


  public static CommandResponse lookupJSONArray(InternalRequestHeader header, CommandPacket commandPacket,
          String guid, String field, String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = signatureAndACLCheckForRead(header, commandPacket, guid, field, 
            null, // fields
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


  public static CommandResponse lookupMultipleValues(InternalRequestHeader header,  CommandPacket commandPacket,
          String guid, String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, commandPacket,
            guid, GNSProtocol.ENTIRE_RECORD.toString(), 
            null, //fields
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


  public static CommandResponse lookupOne(InternalRequestHeader header, CommandPacket commandPacket, 
          String guid, String field,
          String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = signatureAndACLCheckForRead(header, commandPacket, guid, field, 
            null, //fields
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


  public static CommandResponse lookupOneMultipleValues(InternalRequestHeader header, CommandPacket commandPacket,
          String guid, String reader,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {

    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, commandPacket,
            guid, GNSProtocol.ENTIRE_RECORD.toString(), 
            null, //fields
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


  public static ResponseCode update(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid, String key, String value, String oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message,
          Date timestamp,
          ClientRequestHandlerInterface handler) {
    return update(header, commandPacket, guid, key,
            new ResultValue(Arrays.asList(value)),
            oldValue != null ? new ResultValue(Arrays.asList(oldValue)) : null,
            argument,
            operation,
            writer, signature, message, timestamp, handler);
  }


  public static ResponseCode update(InternalRequestHeader header, CommandPacket commandPacket, String guid, String key,
          ResultValue value, ResultValue oldValue,
          int argument, UpdateOperation operation,
          String writer, String signature, String message,
          Date timestamp,
          ClientRequestHandlerInterface handler) {

    try {
      return NSUpdateSupport.executeUpdateLocal(header, commandPacket, guid, key, writer, signature, message,
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


  private static ResponseCode update(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid, JSONObject json, UpdateOperation operation,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    try {
      return NSUpdateSupport.executeUpdateLocal(header, commandPacket, guid, null,
              writer, signature, message, timestamp, operation,
              null, null, -1, new ValuesMap(json), handler.getApp(), false);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException |
            SignatureException | JSONException | IOException | InternalRequestException |
            FailedDBOperationException | RecordNotFoundException | FieldNotFoundException e) {
      LOGGER.log(Level.FINE, "Update threw error: {0}", e);
      return ResponseCode.UPDATE_ERROR;
    }
  }


  public static ResponseCode updateUserJSON(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid, JSONObject json,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    return FieldAccess.update(header, commandPacket, guid, new ValuesMap(json),
            UpdateOperation.USER_JSON_REPLACE,
            writer, signature, message, timestamp, handler);
  }


  public static ResponseCode createField(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid, String key, ResultValue value,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    return update(header, commandPacket, guid, key, value, null, -1,
            UpdateOperation.SINGLE_FIELD_CREATE, writer, signature, message,
            timestamp, handler);
  }


  public static ResponseCode deleteField(InternalRequestHeader header, CommandPacket commandPacket, String guid, String key,
          String writer, String signature, String message,
          Date timestamp, ClientRequestHandlerInterface handler) {
    return update(header, commandPacket, guid, key,
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


  public static CommandResponse selectGroupSetupQuery(InternalRequestHeader header,
          CommandPacket commandPacket,
          String accountGuid, String query, String publicKey,
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
        CommandResponse groupGuidCreateresult = AccountAccess.addGuid(header, commandPacket,
                accountInfo, accountGuidInfo,
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


  public static ResponseCode signatureAndACLCheckForRead(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid,
          String field, List<String> fields,
          String reader, String signature, String message,
          Date timestamp,
          GNSApplicationInterface<String> app) {
    ResponseCode errorCode = ResponseCode.NO_ERROR;
    LOGGER.log(Level.FINEST,
            "signatureAndACLCheckForRead guid: {0} field: {1} reader: {2}",
            new Object[]{guid, field, reader});
    try {
      assert (header != null);

      // Fixme: Not following the logic in here.
      // note: reader can also be null here
      if (!header.verifyInternal() && !commandPacket.getCommandType().isMutualAuth()
              && (field != null || fields != null)) {
        errorCode = NSAuthentication.signatureAndACLCheck(header, guid, field, fields, reader,
                signature, message, MetaDataTypeName.READ_WHITELIST, app);
      } else {
        LOGGER.log(Level.FINEST,
                "reader={0}; internal={1} field={2}; fields={3};",
                new Object[]{reader, header.verifyInternal(), field, fields});

        // internal and mutual auth commands don't need even ACL checks
        if ((header.verifyInternal()
                && (GNSProtocol.INTERNAL_QUERIER.toString().equals(reader)))
                || commandPacket.getCommandType().isMutualAuth()) {
          return ResponseCode.NO_ERROR;
        }
        //Fixme: I'm guessing this case is for active code only.
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
