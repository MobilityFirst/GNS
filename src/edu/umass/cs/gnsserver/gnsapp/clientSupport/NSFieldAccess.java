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

import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import org.json.JSONException;

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
   * Returns the value of a field in a GUID as a ValuesMap.
   *
   * @param guid
   * @param field
   * @param gnsApp
   * @return ResultValue
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ValuesMap lookupJSONFieldLocalNoAuth(String guid, String field,
          GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    return lookupJSONFieldLocalNoAuth(guid, field, gnsApp, true);
  }

  /**
   * Looks up the value of a field in the guid on this NameServer.
   * Active code is handled if handleActiveCode is true.
   *
   * Returns the value of a field in a GUID as a ValuesMap.
   *
   * @param guid
   * @param field
   * @param gnsApp
   * @param handleActiveCode
   * @return ResultValue
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ValuesMap lookupJSONFieldLocalNoAuth(String guid, String field,
          GNSApplicationInterface<String> gnsApp, boolean handleActiveCode)
          throws FailedDBOperationException {
    ValuesMap valuesMap = lookupFieldLocalNoAuth(guid, field, ColumnFieldType.USER_JSON, gnsApp.getDB());
    if (handleActiveCode) {
      valuesMap = NSFieldAccess.handleActiveCode(field, guid, valuesMap, gnsApp);
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
      if (GNSCommandProtocol.ALL_FIELDS.equals(field)) {
        ClientSupportConfig.getLogger().log(Level.FINE, "Field={0} Format={1}",
                new Object[]{field, returnFormat});
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
        if(Util.oneIn(100))
        	DelayProfiler.updateDelayNano("getNameRecordMultiUserFields", t);
      }
      if (nameRecord != null) {
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

  // FIXME: Active code is not handled yet in here.
  public static ValuesMap lookupFieldsLocalNoAuth(String guid, List<String> fields,
          ColumnFieldType returnFormat, BasicRecordMap database)
          throws FailedDBOperationException {
    // Try to look up the value in the database
    try {
      ClientSupportConfig.getLogger().log(Level.FINE, "Fields={0} Format={1}",
              new Object[]{fields, returnFormat});
      String[] fieldArray = new String[fields.size()];
      fieldArray = fields.toArray(fieldArray);
      // Grab the fields the user wanted
      NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(database, guid,
              returnFormat, fieldArray);
      if (nameRecord != null) {
        return nameRecord.getValuesMap();
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
   * Looks up the value of an old-style list field in the guid on this NameServer.
   * Returns the value of a field in a GUID as a ResultValue or
   * an empty ResultValue if field cannot be found.
   *
   * @param guid
   * @param field
   * @param database
   * @return ResultValue
   */
  public static ResultValue lookupListFieldLocallyNoAuth(String guid, String field,
          BasicRecordMap database) {
    ResultValue result = null;
    try {
      // arun: cleaned up logging
      NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(database, guid,
              ColumnFieldType.LIST_STRING, field);
      ClientSupportConfig.getLogger().log(Level.FINE,
              "LOOKUPFIELDONTHISSERVER: {0} : {1} -> {2}",
              new Object[]{guid, field, nameRecord});
      result = nameRecord.getUserKeyAsArray(field);
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
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static String lookupSingletonFieldOnThisServer(String guid, String field,
          BasicRecordMap database) throws FailedDBOperationException {
    ResultValue guidResult = lookupListFieldLocallyNoAuth(guid, field, database);
    if (guidResult != null && !guidResult.isEmpty()) {
      return (String) guidResult.get(0);
    } else {
      return null;
    }
  }

  /**
   * Looks up the value of an old-style list field in the guid.
   * If allowQueryToOtherNSs is true and guid doesn't exists on this Name Server,
   * sends a DNS query from this Name Server to a Local Name Server.
   * Returns the value of a field in a GUID as a ResultValue.
   *
   * @param guid
   * @param field
   * @param allowRemoteQuery
   * @param handler
   * @return ResultValue containing the value of the field or an empty ResultValue if field cannot be found
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ResultValue lookupListFieldAnywhere(String guid, String field,
          boolean allowRemoteQuery, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    ResultValue result = lookupListFieldLocallyNoAuth(guid, field, handler.getApp().getDB());
    // if values wasn't found and the guid doesn't exist on this server 
    // and we're allowed then send a query to another server
    if (result.isEmpty() && !handler.getApp().getDB().containsName(guid) && allowRemoteQuery) {
      try {
        //ClientSupportConfig.getLogger().info("RQ: ");
        String stringResult = handler.getRemoteQuery().fieldReadArray(guid, field);
        result = new ResultValue(stringResult);
      } catch (Exception e) {
        ClientSupportConfig.getLogger().log(Level.SEVERE,
                "Problem getting record from remote server: {0}", e);
      }
      //result = lookupListFieldQueryLNS(guid, field, activeReplica, lnsAddress);
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
   * If allowQueryToOtherNSs is true and guid doesn't exists on this Name Server,
   * sends a DNS query from this Name Server to a Local Name Server.
   * Returns the value of a field in a GUID as a ResultValue.
   *
   * @param guid
   * @param field
   * @param gnsApp
   * @return ValuesMap containing the value of the field or null if field cannot be found
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static ValuesMap lookupJSONFieldAnywhere(String guid, String field,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    ValuesMap result = lookupJSONFieldLocalNoAuth(guid, field, gnsApp);
    // if values wasn't found and the guid doesn't exist on this server and we're allowed then send a query to the LNS
    if (result == null && !gnsApp.getDB().containsName(guid)) {
      try {
        String stringResult = gnsApp.getRequestHandler().getRemoteQuery().fieldRead(guid, field);
        if (stringResult != null) {
          result = new ValuesMap();
          result.put(field, stringResult);
        }
      } catch (IOException | JSONException | ClientException e) {
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

  private static ValuesMap handleActiveCode(String field, String guid,
          ValuesMap originalValues, GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
	  long start = System.nanoTime();
	  if (field.equals("level1")){
	    	return originalValues;
	  }
	  if(!AppReconfigurableNodeOptions.enableActiveCode) return originalValues;
	  
	  ValuesMap newResult = originalValues;	  
	  
	  // Only do this for user fields.
	  if (field == null || !InternalField.isInternalField(field)) {
      int hopLimit = 1;
      DelayProfiler.updateDelayNano("activeIsInternal", start);
      
      
      // Grab the code because it is of a different type
      long t = System.nanoTime();
      NameRecord codeRecord = null;
      try {
        codeRecord = NameRecord.getNameRecordMultiUserFields(gnsApp.getDB(), guid,
                ColumnFieldType.USER_JSON, ActiveCode.ON_READ);
      } catch (RecordNotFoundException e) {
        //GNS.getLogger().severe("Active code read record not found: " + e.getMessage());
      }
      DelayProfiler.updateDelayNano("activeFetchCode", t);
      
      
      t = System.nanoTime();
      if (codeRecord != null && originalValues != null && gnsApp.getActiveCodeHandler() != null
              && gnsApp.getActiveCodeHandler().hasCode(codeRecord, ActiveCode.READ_ACTION)) {
    	  DelayProfiler.updateDelayNano("activeCheckNull", t);
        try {
          t = System.nanoTime();
          String code64 = codeRecord.getValuesMap().getString(ActiveCode.ON_READ);          
          
          ClientSupportConfig.getLogger().log(Level.FINE, "AC--->>> {0} {1} {2}",
                  new Object[]{guid, field, originalValues.toString()});
          DelayProfiler.updateDelayNano("activeGetCodeString", t);
          
          t = System.nanoTime();
          newResult = gnsApp.getActiveCodeHandler().runCode(code64, guid, field,
                  "read", originalValues, hopLimit);          
          ClientSupportConfig.getLogger().log(Level.FINE, "AC--->>> {0}",
                  newResult.toString());
          DelayProfiler.updateDelayNano("activeRunCode", t);
          
        } catch (Exception e) {
          ClientSupportConfig.getLogger().log(Level.FINE, "Active code error: {0}",
                  e.getMessage());
        }
      }
    }
	DelayProfiler.updateDelayNano("activeTotal", start);
    return newResult;
  }
}
