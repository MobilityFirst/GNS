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
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.asynch.ClientAsynchBase;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GroupAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
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
   * Update the members of a group guid.
   *
   * @param guid
   * @param members
   * @param handler
   * @param lnsAddress
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static void updateMembers(String guid, Set<String> members,
          ClientRequestHandlerInterface handler, InetSocketAddress lnsAddress)
          throws ClientException, IOException, JSONException {
    //ClientSupportConfig.getLogger().info("RQ: ");

    String response = handler.getRemoteQuery().fieldReplaceOrCreateArray(guid, GroupAccess.GROUP,
            new ResultValue(members));
//    NSResponseCode groupResponse = LNSUpdateHandler.sendUpdate(guid, GroupAccess.GROUP, new ResultValue(members),
//            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
    // We could roll back the above operation if the one below gets an error, but we don't
    // We'll worry about this when we get transactions working.
    if (response.equals(GNSCommandProtocol.OK_RESPONSE)) {
      //if (!groupResponse.isAnError()) {
      // This is probably a bad idea to update every member
      for (String member : members) {
        handler.getRemoteQuery().fieldReplaceOrCreateArray(member, GroupAccess.GROUPS,
                new ResultValue(Arrays.asList(guid)));
//        LNSUpdateHandler.sendUpdate(member, GroupAccess.GROUPS, new ResultValue(Arrays.asList(guid)),
//                UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, activeReplica, lnsAddress);
      }
    }
  }

  /**
   * Return the members of a the group guid.
   *
   * @param guid
   * @param allowQueryToOtherNSs
   * @param handler
   * @return the members as a {@link ResultValue}
   * @throws FailedDBOperationException
   */
  public static ResultValue lookupMembers(String guid, boolean allowQueryToOtherNSs,
          ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    return NSFieldAccess.lookupListFieldAnywhere(guid, GroupAccess.GROUP, allowQueryToOtherNSs, handler);
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
    return !NSFieldAccess.lookupListFieldLocallyNoAuth(guid, GroupAccess.GROUP, database).isEmpty();
  }

  /**
   * Returns the groups that a GUID is a member of.
   *
   * @param guid
   * @param handler
   * @return a set of strings
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static Set<String> lookupGroups(String guid, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupListFieldAnywhere(guid, GroupAccess.GROUPS, true, handler).toStringSet();
  }

  /**
   * Returns the groups that a GUID is a member of.
   * Doesn't attempt to access another server to find it.
   *
   * @param guid
   * @param handler
   * @return a set of strings
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static Set<String> lookupGroupsOnThisServer(String guid, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupListFieldAnywhere(guid, GroupAccess.GROUPS, false, handler).toStringSet();
  }

  /**
   * Removes from the groupGuid the memberGuid.
   *
   * @param groupGuid
   * @param memberGuid
   * @param handler
   * @return an {@link GNSResponseCode}
   */
  public static GNSResponseCode removeFromGroup(String groupGuid, String memberGuid,
          ClientRequestHandlerInterface handler) {
    try {
      handler.getRemoteQuery().fieldRemove(groupGuid, GroupAccess.GROUP, memberGuid);
//    NSResponseCode groupResponse = LNSUpdateHandler.sendUpdate(groupGuid, GroupAccess.GROUP,
//            new ResultValue(Arrays.asList(memberGuid)),
//            UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica, lnsAddress);
      // We could roll back the above operation if the one below gets an error, but we don't
      // We'll worry about this when we get transactions working.
      handler.getRemoteQuery().fieldRemove(memberGuid, GroupAccess.GROUPS, groupGuid);
//    if (!groupResponse.isAnError()) {
//      LNSUpdateHandler.sendUpdate(memberGuid, GroupAccess.GROUPS, new ResultValue(Arrays.asList(groupGuid)),
//              UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica, lnsAddress);
//    }
      return GNSResponseCode.NO_ERROR;
    } catch (IOException | JSONException | ClientException e) {
      return GNSResponseCode.UNSPECIFIED_ERROR;
    }

  }

  /**
   * Removes all group links when we're deleting a guid.
   *
   * @param guid
   * @param handler
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static void cleanupGroupsForDelete(String guid, ClientRequestHandlerInterface handler) throws FailedDBOperationException {
    for (String groupGuid : lookupGroups(guid, handler)) {
      removeFromGroup(groupGuid, guid, handler);
    }
  }

  ///
  /// Support code for context sensitive group guids
  ///
  /**
   * Updates the last update field for this group guid.
   *
   * @param guid
   * @param lastUpdate
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static void updateLastUpdate(String guid, Date lastUpdate, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException {
    //ClientSupportConfig.getLogger().info("RQ: ");

    handler.getRemoteQuery().fieldUpdate(guid, GROUP_LAST_UPDATE, lastUpdate.getTime());

//    LNSUpdateHandler.sendUpdate(guid, GROUP_LAST_UPDATE, new ResultValue(Arrays.asList(Long.toString(lastUpdate.getTime()))),
//            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
  }

  /**
   * Updates the min refresh interval field for this group guid.
   *
   * @param guid
   * @param minRefresh
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static void updateMinRefresh(String guid, int minRefresh, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException {
    //ClientSupportConfig.getLogger().info("RQ: ");

    handler.getRemoteQuery().fieldUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, minRefresh);
//    LNSUpdateHandler.sendUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, new ResultValue(Arrays.asList(Integer.toString(minRefresh))),
//            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
  }

  /**
   * Updates the query string field for this group guid.
   *
   * @param guid
   * @param queryString
   * @param handler
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static void updateQueryString(String guid, String queryString, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException {
    //ClientSupportConfig.getLogger().info("RQ: ");

    handler.getRemoteQuery().fieldUpdate(guid, GROUP_QUERY_STRING, queryString);
//    LNSUpdateHandler.sendUpdate(guid, GROUP_QUERY_STRING, new ResultValue(Arrays.asList(queryString)),
//            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
  }

  /**
   * Returns the last update time for this group guid.
   *
   * @param guid
   * @param handler
   * @return the last update time
   * @throws FailedDBOperationException
   */
  public static Date getLastUpdate(String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupListFieldAnywhere(guid, GROUP_LAST_UPDATE,
            true, handler);
    ClientSupportConfig.getLogger().log(Level.FINE, "++++ResultValue = {0}", resultValue);
    if (!resultValue.isEmpty()) {
      return new Date(Long.parseLong((String) resultValue.get(0)));
    } else {
      return null;
    }
  }

  /**
   * Returns the min refresh interval for this group guid.
   *
   * @param guid
   * @param handler
   * @return the min refresh interval
   * @throws FailedDBOperationException
   */
  public static int getMinRefresh(String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupListFieldAnywhere(guid, GROUP_MIN_REFRESH_INTERVAL,
            true, handler);
    ClientSupportConfig.getLogger().log(Level.FINE, "++++ResultValue = {0}", resultValue);
    if (!resultValue.isEmpty()) {
      return Integer.parseInt((String) resultValue.get(0));
    } else {
      // if we can't get it just return the default. No harm, no foul.
      return ClientAsynchBase.DEFAULT_MIN_REFRESH_INTERVAL_FOR_SELECT;
    }
  }

  /**
   * Returns the query string for this group guid.
   *
   * @param guid
   * @param handler
   * @return the query string
   * @throws FailedDBOperationException
   */
  public static String getQueryString(String guid, ClientRequestHandlerInterface handler)
          throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupListFieldAnywhere(guid, GROUP_QUERY_STRING,
            true, handler);
    ClientSupportConfig.getLogger().log(Level.FINE, "++++ResultValue = {0}", resultValue);
    if (!resultValue.isEmpty()) {
      return (String) resultValue.get(0);
    } else {
      return null;
    }
  }

  /**
   * Returns the values of a field in a set of guids contained in a group guid.
   * The result is return as an array of values.
   *
   * @param groupGuid
   * @param field
   * @param gnsApp
   * @return a ValuesMap containing the field with an array of the values
   * @throws FailedDBOperationException
   * @throws JSONException
   */
  public static ValuesMap lookupFieldInGroupGuid(String groupGuid, String field,
          GNSApplicationInterface<String> gnsApp) throws FailedDBOperationException, JSONException {
    JSONArray resultArray = new JSONArray();
    for (Object guidObject : lookupMembers(groupGuid, false, gnsApp.getRequestHandler())) {
      String guid = (String) guidObject;
      ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldAnywhere(guid, field, gnsApp);
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
