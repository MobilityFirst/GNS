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

import java.io.IOException;
import java.util.Date;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * GroupAccess provides an interface to the group information in the GNS.
 *
 * The members of a group are stored in a record whose key is the GROUP string.
 *
 * @author westy
 */
public class NSGroupAccess {

  /**
   * The field for group records.
   */
  public static final String GROUP_RECORDS = InternalField.makeInternalFieldString("groupRecords");
  /**
   * The field for group minimum refresh interval.
   */
  public static final String GROUP_MIN_REFRESH_INTERVAL = InternalField.makeInternalFieldString("groupMinRefresh");
  /**
   * The field for group last update time.
   */
  public static final String GROUP_LAST_UPDATE = InternalField.makeInternalFieldString("groupLastUpdate");
  /**
   * The field for group query string.
   */
  public static final String GROUP_QUERY_STRING = InternalField.makeInternalFieldString("groupQueryString");

  /**
   * The default interval (in seconds) before which a query will not be refreshed. In other words
   * if you wait this interval you will get the latest from the database, otherwise you will get the
   * cached value.
   */
  public static final int DEFAULT_MIN_REFRESH_INTERVAL_FOR_SELECT = 60; //seconds
  
  /**
   * Return the members of a the group guid.
   *
   * @param header
   * @param guid
   * @param allowQueryToOtherNSs
   * @param handler
   * @return the members as a {@link ResultValue}
   * @throws FailedDBOperationException
   */
  public static ResultValue lookupMembers(InternalRequestHeader header, String guid, boolean allowQueryToOtherNSs,
          ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    return NSFieldAccess.lookupListFieldAnywhere(header, guid, GroupAccess.GROUP, allowQueryToOtherNSs, handler);
  }

  /**
   * Returns true if the guid is a group guid.
   *
   * @param guid
   * @param database
   * @return true if the guid is a group guid
   * @throws FailedDBOperationException
   */
  public static boolean isGroupGuid(String guid, BasicRecordMap database) throws FailedDBOperationException {
    return !NSFieldAccess.lookupListFieldLocallySafe(guid, GroupAccess.GROUP, database).isEmpty();
  }

  /**
   * Returns the groups that a GUID is a member of.
   *
   * @param header
   * @param guid
   * @param handler
   * @return a set of strings
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static Set<String> lookupGroups(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupListFieldAnywhere(header, guid, GroupAccess.GROUPS, true, handler).toStringSet();
  }

  /**
   * Returns the groups that a GUID is a member of.
   * Doesn't attempt to access another server to find it.
   *
   * @param header
   * @param guid
   * @param handler
   * @return a set of strings
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static Set<String> lookupGroupsOnThisServer(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupListFieldAnywhere(header, guid, GroupAccess.GROUPS, false, handler).toStringSet();
  }
  
  ///
  /// Support code for context sensitive group guids
  ///
  /**
   * Updates the last update field for this group guid.
   *
   * @param header
   * @param guid
   * @param lastUpdate
   * @param handler
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
 * @throws InternalRequestException 
   */
  public static void updateLastUpdate(InternalRequestHeader header, String guid, Date lastUpdate, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {
    //handler.getRemoteQuery().fieldUpdate(guid, GROUP_LAST_UPDATE, lastUpdate.getTime());
	  handler.getInternalClient().execute(GNSCommandInternal.fieldUpdate(guid, GROUP_LAST_UPDATE, lastUpdate.getTime()+"", header));
  }

  /**
   * Updates the min refresh interval field for this group guid.
   *
   * @param header
   * @param guid
   * @param minRefresh
   * @param handler
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
 * @throws InternalRequestException 
   */
  public static void updateMinRefresh(InternalRequestHeader header, String guid, int minRefresh, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {
    //handler.getRemoteQuery().fieldUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, minRefresh);
    handler.getInternalClient().execute(GNSCommandInternal.fieldUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, minRefresh, header));
  }

  /**
   * Updates the query string field for this group guid.
   *
   * @param header
   * @param guid
   * @param queryString
   * @param handler
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
 * @throws InternalRequestException 
   */
  public static void updateQueryString(InternalRequestHeader header, String guid, String queryString, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {
//    handler.getRemoteQuery().fieldUpdate(guid, GROUP_QUERY_STRING, queryString);
    handler.getInternalClient().execute(GNSCommandInternal.fieldUpdate(guid, GROUP_QUERY_STRING, queryString, header));
  }

  /**
   * Returns the last update time for this group guid.
   *
   * @param header
   * @param guid
   * @param handler
   * @return the last update time
   * @throws FailedDBOperationException
   */
  public static Date getLastUpdate(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    Number result = getGroupFieldAsNumber(header, guid, GROUP_LAST_UPDATE, -1, handler);
    if (!result.equals(-1)) {
      return new Date(result.longValue());
    } else {
      return null;
    }
  }

  /**
   * Returns the min refresh interval for this group guid.
   *
   * @param header
   * @param guid
   * @param handler
   * @return the min refresh interval
   * @throws FailedDBOperationException
   */
  public static int getMinRefresh(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    Number result = getGroupFieldAsNumber(header, guid, GROUP_MIN_REFRESH_INTERVAL, -1, handler);
    if (!result.equals(-1)) {
      return result.intValue();
    } else {
      return DEFAULT_MIN_REFRESH_INTERVAL_FOR_SELECT;
    }
  }

  /**
   * Returns the query string for this group guid.
   *
   * @param header
   * @param guid
   * @param handler
   * @return the query string or null if it can't be found
   * @throws FailedDBOperationException
   */
  public static String getQueryString(InternalRequestHeader header, String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    return getGroupFieldAsString(header, guid, GROUP_QUERY_STRING, handler);
  }

  /**
   *
   * @param header
   * @param guid
   * @param field
   * @param handler
   * @return the field
   * @throws FailedDBOperationException
   */
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
  
  /**
   *
   * @param header
   * @param guid
   * @param field
   * @param defaultValue
   * @param handler
   * @return the field
   * @throws FailedDBOperationException
   */
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

  /**
   * Returns the values of a field in a set of guids contained in a group guid.
   * The result is return as an array of values.
   *
   * @param header
   * @param groupGuid
   * @param field
   * @param gnsApp
   * @return a ValuesMap containing the field with an array of the values
   * @throws FailedDBOperationException
   * @throws JSONException
   */
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
