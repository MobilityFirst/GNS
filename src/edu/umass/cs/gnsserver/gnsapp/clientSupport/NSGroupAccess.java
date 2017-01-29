
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GroupAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.logging.Level;


public class NSGroupAccess {


  public static final String GROUP_RECORDS = InternalField.makeInternalFieldString("groupRecords");

  public static final String GROUP_MIN_REFRESH_INTERVAL = InternalField.makeInternalFieldString("groupMinRefresh");

  public static final String GROUP_LAST_UPDATE = InternalField.makeInternalFieldString("groupLastUpdate");

  public static final String GROUP_QUERY_STRING = InternalField.makeInternalFieldString("groupQueryString");


  public static final int DEFAULT_MIN_REFRESH_INTERVAL_FOR_SELECT = 60; //seconds
  

  public static ResultValue lookupMembers(InternalRequestHeader header, String guid, boolean allowQueryToOtherNSs,
          ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    return NSFieldAccess.lookupListFieldAnywhere(header, guid, GroupAccess.GROUP, allowQueryToOtherNSs, handler);
  }


  public static boolean isGroupGuid(String guid, BasicRecordMap database) throws FailedDBOperationException {
    return !NSFieldAccess.lookupListFieldLocallySafe(guid, GroupAccess.GROUP, database).isEmpty();
  }


  public static Set<String> lookupGroups(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupListFieldAnywhere(header, guid, GroupAccess.GROUPS, true, handler).toStringSet();
  }


  public static Set<String> lookupGroupsOnThisServer(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupListFieldAnywhere(header, guid, GroupAccess.GROUPS, false, handler).toStringSet();
  }
  
  ///
  /// Support code for context sensitive group guids
  ///

  public static void updateLastUpdate(InternalRequestHeader header, String guid, Date lastUpdate, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {
    //handler.getRemoteQuery().fieldUpdate(guid, GROUP_LAST_UPDATE, lastUpdate.getTime());
	  handler.getInternalClient().execute(GNSCommandInternal.fieldUpdate(guid, GROUP_LAST_UPDATE, lastUpdate.getTime()+"", header));
  }


  public static void updateMinRefresh(InternalRequestHeader header, String guid, int minRefresh, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {
    //handler.getRemoteQuery().fieldUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, minRefresh);
    handler.getInternalClient().execute(GNSCommandInternal.fieldUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, minRefresh, header));
  }


  public static void updateQueryString(InternalRequestHeader header, String guid, String queryString, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {
//    handler.getRemoteQuery().fieldUpdate(guid, GROUP_QUERY_STRING, queryString);
    handler.getInternalClient().execute(GNSCommandInternal.fieldUpdate(guid, GROUP_QUERY_STRING, queryString, header));
  }


  public static Date getLastUpdate(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    Number result = getGroupFieldAsNumber(header, guid, GROUP_LAST_UPDATE, -1, handler);
    if (!result.equals(-1)) {
      return new Date(result.longValue());
    } else {
      return null;
    }
  }


  public static int getMinRefresh(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    Number result = getGroupFieldAsNumber(header, guid, GROUP_MIN_REFRESH_INTERVAL, -1, handler);
    if (!result.equals(-1)) {
      return result.intValue();
    } else {
      return DEFAULT_MIN_REFRESH_INTERVAL_FOR_SELECT;
    }
  }


  public static String getQueryString(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    return getGroupFieldAsString(header, guid, GROUP_QUERY_STRING, handler);
  }


  public static String getGroupFieldAsString(InternalRequestHeader header, String guid, String field, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldAnywhere(header, guid, field, handler.getApp());
    ClientSupportConfig.getLogger().log(Level.FINE, "++++valuesMap = {0}", valuesMap);
    if (valuesMap.has(field)) {
      try {
        // Something simpler here?
        return (String) valuesMap.get(field);
      } catch (JSONException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Problem parsing GROUP_QUERY_STRING for {0}: {1}", new Object[]{field, e});
      }
    }
    return null;
  }
  

  public static Number getGroupFieldAsNumber(InternalRequestHeader header, String guid, String field, Number defaultValue, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldAnywhere(header, guid, field, handler.getApp());
    ClientSupportConfig.getLogger().log(Level.FINE, "++++valuesMap = {0}", valuesMap);
    if (valuesMap.has(field)) {
      try {
        // Something simpler here?
        return (Number) valuesMap.get(field);
      } catch (JSONException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Problem parsing GROUP_QUERY_STRING for {0}: {1}", new Object[]{field, e});
      }
    }
    return defaultValue;
  }


  public static ValuesMap lookupFieldInGroupGuid(InternalRequestHeader header, String groupGuid, String field,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException, JSONException {
    JSONArray resultArray = new JSONArray();
    for (Object guidObject : lookupMembers(header, groupGuid, false, gnsApp.getRequestHandler())) {
      String guid = (String) guidObject;
      ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldAnywhere(header, guid, field, gnsApp);
      if (valuesMap != null && valuesMap.has(field)) {
        resultArray.put(valuesMap.get(field));
      }
    }
    ClientSupportConfig.getLogger().log(Level.FINE,
            "Group result for {0}/{1} = {2}",
            new Object[]{groupGuid, field, resultArray.toString()});
    ValuesMap result = new ValuesMap();
    result.put(field, resultArray);
    return result;
  }
}
