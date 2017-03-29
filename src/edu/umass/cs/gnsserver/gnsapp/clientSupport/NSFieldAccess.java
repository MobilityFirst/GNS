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
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Methods for reading field information in guids on NameServers.
 *
 * Similar code for getting values from the database exists in GnsReconLookup.
 *
 * @author westy, arun
 */
public class NSFieldAccess {

  /**
   * Looks up the value of a field in the guid on this NameServer.
   * Active code is automatically handled during this call.
   *
   * Returns the value of a field in a GNSProtocol.GUID.toString() as a ValuesMap.
   *
   * @param header
   *
   * @param guid
   * @param field
   * @param gnsApp
   * @return ResultValue
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ValuesMap lookupJSONFieldLocally(InternalRequestHeader header, String guid, String field,
          GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    return lookupJSONFieldLocalNoAuth(header, guid, field, gnsApp, true);
  }

  /**
   * Looks up the value of a field in the guid on this NameServer.
   * Active code is handled if handleActiveCode is true.
   *
   * Returns the value of a field in a GNSProtocol.GUID.toString() as a ValuesMap.
   *
   * @param header
   * @param guid
   * @param field
   * @param gnsApp
   * @param handleActiveCode
   * @return ResultValue
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ValuesMap lookupJSONFieldLocalNoAuth(InternalRequestHeader header, String guid, String field,
          GNSApplicationInterface<String> gnsApp, boolean handleActiveCode)
          throws FailedDBOperationException {
    ValuesMap valuesMap = lookupFieldLocalNoAuth(guid, field, ColumnFieldType.USER_JSON, gnsApp.getDB());
    if (handleActiveCode) {
      try {
        JSONObject result = ActiveCodeHandler.handleActiveCode(header, guid, field, ActiveCode.READ_ACTION, valuesMap, gnsApp.getDB());
        valuesMap = result != null ? new ValuesMap(result) : valuesMap;
      } catch (InternalRequestException e) {
        // Active code field lookup failed, do nothing and return the original value
      }
    }
    return valuesMap;
  }

  private static ValuesMap lookupFieldLocalNoAuth(String guid, String field, ColumnFieldType returnFormat,
          BasicRecordMap database) throws FailedDBOperationException {
    NameRecord nameRecord = null;
    ClientSupportConfig.getLogger().log(Level.FINE,
            "XXXXXXXXXXXXXXXXXXXXX LOOKUP_FIELD_LOCALLY: {0} : {1}",
            new Object[]{guid, field});
    // Try to look up the value in the database
    try {
      // Check for the case where we're returning all the fields the entire record.
      if (GNSProtocol.ENTIRE_RECORD.toString().equals(field)) {
        ClientSupportConfig.getLogger().log(Level.FINE, "ALL FIELDS: Format={0}",
                new Object[]{returnFormat});
        // need everything so just grab all the fields
        nameRecord = NameRecord.getNameRecord(database, guid);
        // Otherwise if field is specified we're just looking up that single field.
      } else if (field != null) {
        ClientSupportConfig.getLogger().log(Level.FINE, "Field={0} Format={1}",
                new Object[]{field, returnFormat});
        long t = System.nanoTime();
        // otherwise grab the field the user wanted
        nameRecord = NameRecord.getNameRecordMultiUserFields(database, guid,
                returnFormat, field);
        if (Util.oneIn(100)) {
          DelayProfiler.updateDelayNano("getNameRecordMultiUserFields", t);
        }
      }
      if (nameRecord != null) {
        ClientSupportConfig.getLogger().log(Level.FINE, "VALUES MAP={0}",
                new Object[]{nameRecord.getValuesMap().toString()});
        return nameRecord.getValuesMap();
      }
    } catch (RecordNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE, "Record not found for name: {0} Key = {1}",
              new Object[]{guid, field});
    } catch (FieldNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE, "Field not found {0} : {1}",
              new Object[]{guid, e});
    }
    return null;
  }

  /**
   *
   * @param header
   * @param guid
   * @param fields
   * @param returnFormat
   * @param handler
   * @return a values map
   * @throws FailedDBOperationException
   */
  public static ValuesMap lookupFieldsLocalNoAuth(InternalRequestHeader header, String guid, List<String> fields,
          ColumnFieldType returnFormat, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    // Try to look up the value in the database
    try {
      ClientSupportConfig.getLogger().log(Level.FINE, "Fields={0} Format={1}",
              new Object[]{fields, returnFormat});
      String[] fieldArray = new String[fields.size()];
      fieldArray = fields.toArray(fieldArray);
      // Grab the fields the user wanted
      NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(handler.getApp().getDB(), guid,
              returnFormat, fieldArray);

      if (nameRecord != null) {
        // active code handling
        ValuesMap valuesMap = nameRecord.getValuesMap();
        // Already checked inside ActiveCodeHandler.handleActiveCode
        //if (!Config.getGlobalBoolean(GNSConfig.GNSC.DISABLE_ACTIVE_CODE)) {
          try {
            JSONObject result = ActiveCodeHandler.handleActiveCode(header, guid, null, ActiveCode.READ_ACTION,
                    valuesMap, handler.getApp().getDB());
            valuesMap = result != null ? new ValuesMap(result) : valuesMap;
          } catch (InternalRequestException e) {
            e.printStackTrace();
          }
        //}
        return valuesMap;
      }
    } catch (RecordNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE, "Record not found for name: {0}", guid);
    } catch (FieldNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE, "Field not found for {0} : {1}",
              new Object[]{guid, e});
    }
    return null;
  }

  /**
   * Looks up the value of an old-style list field
   * in the guid in the local replica.
   * Returns the value of a field in a GNSProtocol.GUID.toString() as a ResultValue or
   * an empty ResultValue if field cannot be found.
   *
   * @param guid
   * @param field
   * @param database
   * @return ResultValue
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   * @throws edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException
   * @throws edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException
   */
  public static ResultValue lookupListFieldLocallyNoAuth(String guid, String field,
          BasicRecordMap database)
          throws FailedDBOperationException, FieldNotFoundException, RecordNotFoundException {
    NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(database, guid,
            ColumnFieldType.LIST_STRING, field);
    ClientSupportConfig.getLogger().log(Level.FINE,
            "LOOKUPFIELDONTHISSERVER: {0} : {1} -> {2}",
            new Object[]{guid, field, nameRecord});
    ResultValue result = nameRecord.getUserKeyAsArray(field);

    if (result != null) {
      return result;
    } else {
      return new ResultValue();
    }
  }

  /**
   * Looks up the value of an old-style list field
   * in the guid in the local replica. Differs from lookupListFieldLocally
   * in that it doesn't throw any exceptions and just returns an empty result
   * if there are exceptions.
   * Returns the value of a field in a GNSProtocol.GUID.toString() as a ResultValue or
   * an empty ResultValue if field cannot be found.
   *
   * @param guid
   * @param field
   * @param database
   * @return ResultValue
   */
  public static ResultValue lookupListFieldLocallySafe(String guid, String field,
          BasicRecordMap database) {
    ResultValue result = null;
    try {
      result = lookupListFieldLocallyNoAuth(guid, field, database);
    } catch (FieldNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Field not found {0} : {1}", new Object[]{guid, field});
    } catch (RecordNotFoundException e) {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Record not found {0} : {1}",
              new Object[]{guid, field});
    } catch (FailedDBOperationException e) {
      ClientSupportConfig.getLogger().log(Level.FINE,
              "Failed DB operation {0} : {1}",
              new Object[]{guid, field});
    }
    if (result != null) {
      return result;
    } else {
      return new ResultValue();
    }
  }

  /**
   * Looks up the first element of field in the guid on this NameServer as a String.
   * Returns null if the field or the record cannot be found.
   *
   * @param guid
   * @param field
   * @param database
   * @return a string representing the first value in field
   */
  public static String lookupSingletonFieldLocal(String guid, String field,
          BasicRecordMap database) {
    ResultValue guidResult = lookupListFieldLocallySafe(guid, field, database);
    if (guidResult != null && !guidResult.isEmpty()) {
      return (String) guidResult.get(0);
    } else {
      return null;
    }
  }

  /**
   * Looks up the value of an old-style list field in the guid.
   * If allowQueryToOtherNSs is true and guid doesn't exists on this Name Server,
   * sends a read query from this Name Server to a Local Name Server.
   * Returns the value of a field in a GNSProtocol.GUID.toString() as a ResultValue.
   *
   * @param guid
   * @param field
   * @param allowRemoteLookup
   * @param handler
   * @return ResultValue containing the value of the field or an empty ResultValue if field cannot be found
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ResultValue lookupListFieldAnywhere(InternalRequestHeader header, String guid, String field,
          boolean allowRemoteLookup, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    ResultValue result = lookupListFieldLocallySafe(guid, field, handler.getApp().getDB());
    // if values wasn't found and the guid doesn't exist on this server 
    // and we're allowed then send a query to another server
    if (result.isEmpty() && !handler.getApp().getDB().containsName(guid) && allowRemoteLookup) {
      try {
        String stringResult = handler.getInternalClient().execute(GNSCommandInternal.fieldRead(guid, field, header)).getResultString();
        result = new ResultValue(stringResult);
      } catch (Exception e) {
        ClientSupportConfig.getLogger().log(Level.SEVERE,
                "Problem getting record from remote server: {0}", e);
      }
      if (!result.isEmpty()) {
        ClientSupportConfig.getLogger().log(Level.FINE, "@@@@@@ Field {0} in {1}"
                + " not found on this server but was found thru remote query. "
                + "Returning {2}",
                new Object[]{field, guid, result.toString()});
      }
    }
    return result;
  }

  /**
   * Looks up the value of a field in the guid.
   * If guid doesn't exists on this Name Server,
   * sends a read query from this Name Server to a Local Name Server.
   * Returns the value of a field in a GNSProtocol.GUID.toString() as a ValuesMap
   *
   * @param header
   * @param guid
   * @param field
   * @param gnsApp
   * @return ValuesMap containing the value of the field or null if field cannot be found
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ValuesMap lookupJSONFieldAnywhere(InternalRequestHeader header, String guid, String field,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    ValuesMap result = lookupJSONFieldLocally(null, guid, field, gnsApp);
    // if values wasn't found and the guid doesn't exist on this server and we're allowed then send a query to the LNS
    if (result == null && !gnsApp.getDB().containsName(guid)) {
      try {
    	  String stringResult = gnsApp.getRequestHandler().getInternalClient().execute(GNSCommandInternal.fieldRead(guid, field, header)).getResultString();
        if (stringResult != null) {
          result = new ValuesMap();
          result.put(field, stringResult);
        }
      } catch (IOException | JSONException | ClientException | InternalRequestException e) {
        ClientSupportConfig.getLogger().log(Level.SEVERE,
                "Problem getting record from remote server: {0}", e);
      }
      if (result != null) {
        ClientSupportConfig.getLogger().log(Level.FINE,
                "@@@@@@ Field {0} in {1} not found on this server but was found thru remote query.",
                new Object[]{field, guid});
      }
    }
    return result;
  }

}
