/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;

import edu.umass.cs.utils.Config;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Date;
import java.util.logging.Level;

import org.apache.commons.lang3.time.DateUtils;
import org.json.JSONException;

/**
 *
 * @author Westy
 */
public class NSUpdateSupport {

  private static final int OLD_COMMAND_TIME = -30; // how far back is old?

  /**
   * Executes a local updateEntireValuesMap operation.
   *
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
   */
  public static GNSResponseCode executeUpdateLocal(InternalRequestHeader header, String guid, String field,
          String writer, String signature, String message, Date timestamp,
          UpdateOperation operation, ResultValue updateValue, ResultValue oldValue, int argument,
          ValuesMap userJSON, GNSApplicationInterface<String> app, boolean doNotReplyToClient)
          throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException,
          SignatureException, JSONException, IOException, FailedDBOperationException,
          RecordNotFoundException, FieldNotFoundException {
    ClientSupportConfig.getLogger().log(Level.FINE,
            "Processing local update {0} / {1} {2} {3}",
            new Object[]{guid, field, operation, updateValue});
    GNSResponseCode errorCode = GNSResponseCode.NO_ERROR;
    // writer will be the INTERNAL_OP_SECRET for internal system reads
    // Fixme: get rid of null writer uses
    if (writer != null && !writer.equals(Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET))) {
      if (field != null) {
        
        errorCode = NSAuthentication.signatureAndACLCheck(guid,
                field, null,
                writer, signature, message, MetaDataTypeName.WRITE_WHITELIST, app);
      } else if (userJSON != null) {
        errorCode = NSAuthentication.signatureAndACLCheck(guid,
                null, userJSON.getKeys(),
                writer, signature, message, MetaDataTypeName.WRITE_WHITELIST, app);
      } else {
        ClientSupportConfig.getLogger().log(Level.FINE,
                "Name {0} key={1} : ACCESS_ERROR", new Object[]{guid, field});
        return GNSResponseCode.ACCESS_ERROR;
      }
    } else {
    }
    // Check for stale commands.
    if (timestamp != null) {
      if (timestamp.before(DateUtils.addMinutes(new Date(), OLD_COMMAND_TIME))) {
        errorCode = GNSResponseCode.STALE_COMMAND_VALUE;
      }
    }
    // return an error packet if one of the checks doesn't pass
    if (errorCode.isExceptionOrError()) {
      return errorCode;
    }
    if (!operation.equals(UpdateOperation.CREATE_INDEX)) {
      // Handle usual case
      NameRecord nameRecord = getNameRecord(guid, field, operation, app.getDB());
      updateNameRecord(header, nameRecord, guid, field, operation, updateValue, oldValue, argument, userJSON,
              app.getDB(), app.getActiveCodeHandler());
      return GNSResponseCode.NO_ERROR;
    } else // Handle special case of a create index
     if (!updateValue.isEmpty() && updateValue.get(0) instanceof String) {
        ClientSupportConfig.getLogger().log(Level.FINE,
                "Creating index for {0} {1}", new Object[]{field, updateValue});
        app.getDB().createIndex(field, (String) updateValue.get(0));

        return GNSResponseCode.NO_ERROR;
      } else {
        ClientSupportConfig.getLogger().log(Level.SEVERE, "Invalid index value:{0}", updateValue);
        return GNSResponseCode.UPDATE_ERROR;
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
          ValuesMap userJSON, BasicRecordMap db, ActiveCodeHandler activeCodeHandler) throws FailedDBOperationException, FieldNotFoundException {
    ValuesMap newValue = null;
    if (activeCodeHandler != null) {
      try {
        newValue = handleActiveCode(header, guid, field, userJSON, db, activeCodeHandler);
      } catch (JSONException e) {
        ClientSupportConfig.getLogger().log(Level.SEVERE,
                "JSON problem while handling active code: {0}", e);
      }
    }
    if (newValue == null) {
      newValue = userJSON;
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
  }

  private static ValuesMap handleActiveCode(InternalRequestHeader header, String guid, String field, ValuesMap userJSON, BasicRecordMap db, ActiveCodeHandler activeCodeHandler) throws FailedDBOperationException, FieldNotFoundException, JSONException {
    // Only do active field handling for user fields.
    if (field == null || !InternalField.isInternalField(field)) {
      NameRecord activeCodeNameRecord = null;
      try {
        activeCodeNameRecord = NameRecord.getNameRecordMultiUserFields(db, guid,
                ColumnFieldType.USER_JSON, ActiveCode.ON_WRITE);
      } catch (RecordNotFoundException e) {
      }
      if (activeCodeNameRecord != null) {
        ClientSupportConfig.getLogger().log(Level.FINE, "AC--->>> {0}", activeCodeNameRecord.toString());
      }
      ValuesMap codeMap = null;
		try {
			codeMap = activeCodeNameRecord.getValuesMap();
		} catch (FieldNotFoundException e) {
			// do nothing
		}
      int hopLimit = 1;
      if (activeCodeNameRecord != null
              && activeCodeHandler.hasCode(codeMap, ActiveCode.WRITE_ACTION)) {
        String code = codeMap.getString(ActiveCode.ON_WRITE);
        ValuesMap packetValuesMap = userJSON;
        ClientSupportConfig.getLogger().log(Level.FINE, "AC--->>> {0} {1} {2}", new Object[]{guid, field, packetValuesMap.toReasonableString()});
        return activeCodeHandler.runCode(header, code, guid, field, "write", packetValuesMap, hopLimit);
      }
    }
    return null;
  }

}
