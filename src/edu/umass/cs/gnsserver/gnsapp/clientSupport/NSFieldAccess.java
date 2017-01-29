
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.FieldNotFoundException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.exceptions.server.RecordNotFoundException;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;


public class NSFieldAccess {


  public static ValuesMap lookupJSONFieldLocally(InternalRequestHeader header, String guid, String field,
          GNSApplicationInterface<String> gnsApp)
          throws FailedDBOperationException {
    return lookupJSONFieldLocalNoAuth(header, guid, field, gnsApp, true);
  }


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


  public static ResultValue lookupListFieldLocallyNoAuth(String guid, String field,
          BasicRecordMap database)
          throws FailedDBOperationException, FieldNotFoundException, RecordNotFoundException {
    ResultValue result = null;
    NameRecord nameRecord = NameRecord.getNameRecordMultiUserFields(database, guid,
            ColumnFieldType.LIST_STRING, field);
    ClientSupportConfig.getLogger().log(Level.FINE,
            "LOOKUPFIELDONTHISSERVER: {0} : {1} -> {2}",
            new Object[]{guid, field, nameRecord});
    result = nameRecord.getUserKeyAsArray(field);

    if (result != null) {
      return result;
    } else {
      return new ResultValue();
    }
  }


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


  public static String lookupSingletonFieldLocal(String guid, String field,
          BasicRecordMap database) {
    ResultValue guidResult = lookupListFieldLocallySafe(guid, field, database);
    if (guidResult != null && !guidResult.isEmpty()) {
      return (String) guidResult.get(0);
    } else {
      return null;
    }
  }


  public static ResultValue lookupListFieldAnywhere(InternalRequestHeader header, String guid, String field,
          boolean allowRemoteQuery, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    ResultValue result = lookupListFieldLocallySafe(guid, field, handler.getApp().getDB());
    // if values wasn't found and the guid doesn't exist on this server 
    // and we're allowed then send a query to another server
    if (result.isEmpty() && !handler.getApp().getDB().containsName(guid) && allowRemoteQuery) {
      try {
        //ClientSupportConfig.getLogger().info("RQ: ");
        //String stringResult = handler.getRemoteQuery().fieldReadArray(guid, field);
        String stringResult = handler.getInternalClient().execute(GNSCommandInternal.fieldRead(guid, field, header)).getResultString();
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


  public static ValuesMap lookupJSONFieldAnywhere(InternalRequestHeader header, String guid, String field,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    ValuesMap result = lookupJSONFieldLocally(null, guid, field, gnsApp);
    // if values wasn't found and the guid doesn't exist on this server and we're allowed then send a query to the LNS
    if (result == null && !gnsApp.getDB().containsName(guid)) {
      try {
        //String stringResult = gnsApp.getRequestHandler().getRemoteQuery().fieldRead(guid, field);
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
