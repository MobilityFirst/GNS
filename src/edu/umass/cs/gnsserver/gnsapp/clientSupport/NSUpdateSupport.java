/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.*;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.Config;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;

import org.apache.commons.lang3.time.DateUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Westy
 */
public class NSUpdateSupport {

  /**
   * Executes a local updateEntireValuesMap operation.
   *
   * @param header
   * @param commandPacket
   * @param guid
   * @param field
   * @param writer
   * @param signature
   * @param message
   * @param timestamp
   * @param operation
   * @param updateValue
   * @param oldValue
   * @param argument
   * @param userJSON
   * @param app
   * @param doNotReplyToClient
   * @return an NSResponseCode
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   * @throws InvalidKeyException
   * @throws SignatureException
   * @throws JSONException
   * @throws IOException
   * @throws FailedDBOperationException
   * @throws RecordNotFoundException
   * @throws FieldNotFoundException
   * @throws edu.umass.cs.gnscommon.exceptions.server.InternalRequestException
   */
  public static ResponseCode executeUpdateLocal(InternalRequestHeader header, CommandPacket commandPacket, String guid, String field,
          String writer, MetaDataTypeName access, String signature, String
													message, Date timestamp,
          UpdateOperation operation, ResultValue updateValue, ResultValue oldValue, int argument,
          ValuesMap userJSON, GNSApplicationInterface<String> app, boolean doNotReplyToClient)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException,
          SignatureException, JSONException, IOException, FailedDBOperationException,
          RecordNotFoundException, FieldNotFoundException, InternalRequestException {
    ResponseCode errorCode = ResponseCode.NO_ERROR;
    assert (header != null);
    if(access==null) access = MetaDataTypeName.WRITE_WHITELIST;
    // No checks for local non-auth commands like verifyAccount or for mutual auth
    if (!GNSProtocol.INTERNAL_QUERIER.toString().equals(writer)
            && !commandPacket.getCommandType().isMutualAuth()) {
      if (!header.verifyInternal()) {
        // This the standard auth check for most updates
        if (field != null) {
          errorCode = NSAuthentication.signatureAndACLCheck(header, guid, field, null,
                  writer, signature, message, access, app);
        } else if (userJSON != null) {
          errorCode = NSAuthentication.signatureAndACLCheck(header, guid, null, userJSON.getKeys(),
                  writer, signature, message, access, app);
        } else {
          ClientSupportConfig.getLogger().log(Level.FINE,
                  "Name {0} key={1} : ACCESS_ERROR", new Object[]{guid, field});
          return ResponseCode.ACCESS_ERROR;
        }
      } else {
        // This ACL check will be only used for active code remote query 
        if (field != null) {
          assert (header.getQueryingGUID() != null) : guid + ":" + field + ":" + writer + "::" + header.getOriginatingGUID();
          errorCode = NSAuthentication.aclCheck(header, guid, field, header.getQueryingGUID(), MetaDataTypeName.WRITE_WHITELIST, app).getResponseCode();
        } else if (userJSON != null) {
          List<String> fields = userJSON.getKeys();
          for (String aField : fields) {
            AclCheckResult aclResult = NSAuthentication.aclCheck(header, guid, aField, header.getQueryingGUID(), MetaDataTypeName.WRITE_WHITELIST, app);
            if (aclResult.getResponseCode().isExceptionOrError()) {
              errorCode = aclResult.getResponseCode();
            }
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
    // Return an error code if one of the checks doesn't pass
    if (errorCode.isExceptionOrError()) {
      return errorCode;
    }
    if (!operation.equals(UpdateOperation.CREATE_INDEX)) {
      // Handle usual case
      NameRecord nameRecord = getNameRecord(guid, field, operation, app.getDB());
      updateNameRecord(header, nameRecord, guid, field, operation, updateValue, oldValue, argument, userJSON,
              app.getDB(), app.getActiveCodeHandler());
      return ResponseCode.NO_ERROR;
    } else // Handle special case of a create index
     if (!updateValue.isEmpty() && updateValue.get(0) instanceof String) {
        ClientSupportConfig.getLogger().log(Level.FINE,
                "Creating index for {0} {1}", new Object[]{field, updateValue});
        app.getDB().createIndex(field, (String) updateValue.get(0));

        return ResponseCode.NO_ERROR;
      } else {
        ClientSupportConfig.getLogger().log(Level.SEVERE, "Invalid index value:{0}", updateValue);
        return ResponseCode.UPDATE_ERROR;
      }
  }

  private static NameRecord getNameRecord(String guid, String field, UpdateOperation operation, BasicRecordMap db) throws RecordNotFoundException, FailedDBOperationException {
    if (operation.isAbleToSkipRead()) {
      // some operations don't require a read first
      return new NameRecord(db, guid);
    } else //try {
     if (field == null) {
        return NameRecord.getNameRecord(db, guid);
      } else {
        return NameRecord.getNameRecordMultiUserFields(db, guid,
                ColumnFieldType.LIST_STRING, field);
      }
  }

  private static void updateNameRecord(InternalRequestHeader header, NameRecord nameRecord, String guid, String field,
          UpdateOperation operation, ResultValue updateValue, ResultValue oldValue, int argument,
          ValuesMap userJSON, BasicRecordMap db, ActiveCodeHandler activeCodeHandler)
          throws FailedDBOperationException, FieldNotFoundException, InternalRequestException {
    ValuesMap newValue = userJSON;
    if (activeCodeHandler != null) {
      JSONObject result = ActiveCodeHandler.handleActiveCode(header, guid, field, ActiveCode.WRITE_ACTION, userJSON, db);
      newValue = result != null ? new ValuesMap(result) : null;
    }
    // END ACTIVE CODE HANDLING
    if (field != null) {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "field={0}, operation={1}, value={2}, name_record={3}",
              new Object[]{field, operation, updateValue,
                nameRecord.getSummary()});
    }
    // Apply updateEntireValuesMap to record in the database
    nameRecord.updateNameRecord(field, updateValue, oldValue, argument, newValue, operation);
    
    // This is for MOB-893 - logging updates
    if(Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_UPDATE_LOGGING))
    	writeUpdateLog(guid, field, updateValue, newValue, operation);

	  // START: Check for triggers
	  {
	  	// gather fields being updated
		  String[] fields = (field != null ? new String[]{field} : null);
		  try {
			  if (fields == null && userJSON != null)
				  fields = userJSON.getKeys().toArray(new String[0]);
		  } catch (JSONException je) {
			  je.printStackTrace();
		  }

		  // for each field, check for a corresponding trigger
		  if (fields != null) for (String updatedField : fields)
			  if (Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_TRIGGERS)) {
				  JSONObject metadata = NSAccessSupport.getMetaDataForACLCheck
					  (guid, db);
				  if (!updatedField.contains(MetaDataTypeName.TRIGGER_LIST
					  .getFieldPath())) {
				  	// field corresponding to trigger list
					  String key = FieldMetaData.makeFieldMetaDataKey
						  (MetaDataTypeName.TRIGGER_LIST, updatedField)
						  .replaceFirst("\\.MD$", "");

					  String[] parts = key.split("\\.");
					  boolean found = false;

					  try {
					  	// match field being updated as deeply as possible
						  // with matching trigger list
						  for (String part : parts) {
							  if (metadata.has(part)) {
								  metadata = metadata.getJSONObject(part);
								  found = true;
							  }
							  else break;
						  }
						  if (found) {
							  // found match; notify
							  JSONArray notifieesInfo = metadata.getJSONArray
								  (GNSProtocol.MD.toString());
							  sendNotifications(guid, updatedField,
								  notifieesInfo);
						  }

					  } catch (JSONException je) {
						  je.printStackTrace();
						  //notification is best effort, so just move on
					  }
				  }
			  }
	  } // END: check for triggers

  }

  private static DatagramSocket udpSocket = null;
  private static final int TRIGGERED_RESPONSE_SIZE = 128;
  private static final String TRIGGERED_RESPONSE_ENCODING = "ISO-8859-1";

  private static synchronized void initUDPSocket() throws SocketException {
  	if(udpSocket==null)
  		udpSocket = new DatagramSocket();
  }

	// send triggered notifications
	private static void sendNotifications(String guid, String field, JSONArray
		notifieesInfo) {
		try {
			initUDPSocket();
			JSONObject response = new JSONObject().put(GNSProtocol.GUID
				.toString(), guid).put(GNSProtocol.FIELD.toString(), field)
				.put(GNSProtocol.ASYNC_RESPONSE_TYPE.toString(), GNSProtocol
					.TRIGGERED_NOTIFICATION.toString());

			for (int i = 0; i < notifieesInfo.length(); i++) {
				JSONObject notifiee = notifieesInfo.getJSONObject(i);
				byte[] responseBytes = notifiee.toString().getBytes
					(TRIGGERED_RESPONSE_ENCODING);
				DatagramPacket datagramPacket = new DatagramPacket
					(responseBytes, 0, responseBytes.length);
				datagramPacket.setAddress(InetAddress.getByName(notifiee
					.getString(GNSProtocol.TRIGGER_IP.toString())));
				datagramPacket.setPort(notifiee.getInt(GNSProtocol
					.TRIGGER_PORT.toString()));
				udpSocket.send(datagramPacket);
			}
		}            // notification is best effort, so move on for now
		catch (IOException se) {
			se.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	// This is for MOB-893 - logging updates
  private static void writeUpdateLog(String guid, String field,
          ResultValue updateValue, ValuesMap userJSON,
          UpdateOperation operation) {
    try {
      if (field == null) {

        for (String singleField : userJSON.getKeys()) {
          if (!InternalField.isInternalField(singleField)) {
            writeFieldLogEntry(guid, singleField, userJSON.get(singleField), operation);
          }
        }
      } else if (!InternalField.isInternalField(field)) {
        writeFieldLogEntry(guid, field, updateValue, operation);
      }
    } catch (JSONException e) {
      ClientSupportConfig.getLogger().log(Level.WARNING, "Unable to log update: {0}", e);
    }
  }

  // This is for MOB-893 - logging updates
  private static void writeFieldLogEntry(String guid, String field, Object value,
          UpdateOperation operation) throws JSONException {
    // write it out as json
    JSONObject json = new JSONObject();
    json.put("guid", guid);
    json.put("field", field);
    json.put("value", value);
    json.put("operation", operation.name());
    ClientSupportConfig.getLogger().log(Level.INFO, "Field update: {0}", json.toString());
//    ClientSupportConfig.getLogger().log(Level.INFO,
//            "Field update: '{'guid : {0}, field: {1}, value: {2}, operation: {3}'}'",
//            new Object[]{guid, field, value, operation});
  }

}
