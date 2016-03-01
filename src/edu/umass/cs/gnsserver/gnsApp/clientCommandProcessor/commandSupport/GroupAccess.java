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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.main.GNS;
import java.io.IOException;
import java.util.Arrays;
import org.json.JSONException;

//import edu.umass.cs.gnsserver.packet.QueryResultValue;
/**
 * GroupAccess provides an interface to the group information in the GNS.
 *
 * The members of a group are stored in a record in each guid whose key is the GROUP string.
 * There is also a "reverse" link (GROUPS) stored in each guid which is the groups that the guid is a member of.
 * The reverse link means that we can check for membership of a guid without going to a different NS.
 *
 * @author westy
 */
public class GroupAccess {

  /**
   * Hidden field that stores group members
   */
  public static final String GROUP = InternalField.makeInternalFieldString("group");
  /**
   * Hidden field that stores what groups a GUID is a member of
   */
  public static final String GROUPS = InternalField.makeInternalFieldString("groups");

  /**
   * Sends a request to the NS to add a single GUID to a group.
   *
   * @param guid
   * @param memberGuid
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return a response code
   */
  public static NSResponseCode addToGroup(String guid, String memberGuid, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) throws IOException, JSONException, GnsClientException {

    handler.getRemoteQuery().fieldAppendToArray(guid, GROUP, new ResultValue(Arrays.asList(memberGuid)));
    handler.getRemoteQuery().fieldAppendToArray(memberGuid, GROUPS, new ResultValue(Arrays.asList(guid)));
    return NSResponseCode.NO_ERROR;
  }

  /**
   * Sends a request to the NS to add a list of GUIDs to a group.
   *
   * @param guid
   * @param members
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return a response code
   */
  public static NSResponseCode addToGroup(String guid, ResultValue members, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) throws GnsClientException, IOException, JSONException {
    handler.getRemoteQuery().fieldAppendToArray(guid, GROUP, members);
    for (String memberGuid : members.toStringSet()) {
      handler.getRemoteQuery().fieldAppendToArray(memberGuid, GROUPS, new ResultValue(Arrays.asList(guid)));
    };
    return NSResponseCode.NO_ERROR;
  }

  /**
   * Sends a request to the NS to remove a single GUID from a group.
   *
   * @param guid
   * @param memberGuid
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return a response code
   * @throws edu.umass.cs.gnscommon.exceptions.client.GnsClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static NSResponseCode removeFromGroup(String guid, String memberGuid, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) throws GnsClientException, IOException, JSONException {
    handler.getRemoteQuery().fieldRemove(guid, GroupAccess.GROUP, memberGuid);
    handler.getRemoteQuery().fieldRemove(memberGuid, GroupAccess.GROUPS, guid);
    return NSResponseCode.NO_ERROR;
  }

  /**
   * Sends a request to the NS to remove a list of GUIDs from a group.
   *
   * @param guid
   * @param members
   * @param writer
   * @param signature
   * @param message
   * @param handler
   * @return a response code
   * @throws edu.umass.cs.gnscommon.exceptions.client.GnsClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static NSResponseCode removeFromGroup(String guid, ResultValue members, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) throws GnsClientException, IOException, JSONException {
    handler.getRemoteQuery().fieldRemoveMultiple(guid, GroupAccess.GROUP, members);
    for (String memberGuid : members.toStringSet()) {
      handler.getRemoteQuery().fieldRemove(memberGuid, GroupAccess.GROUPS, guid);
    }
    return NSResponseCode.NO_ERROR;
  }

  /**
   * Returns the members of the group GUID.
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @return a response code
   */
  public static ResultValue lookup(String guid, String reader, String signature, String message,
          ClientRequestHandlerInterface handler) {
    NSResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(guid, GROUP,
            reader, signature, message, handler.getApp());
    if (errorCode.isAnError()) {
      return new ResultValue();
    }
    return NSFieldAccess.lookupListFieldLocallyNoAuth(guid, GROUP, handler.getApp().getDB());
  }

  /**
   * Returns the groups that a GUID is a member of.
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @param handler
   * @return a response code
   */
  public static ResultValue lookupGroupsAnywhere(String guid, String reader, String signature, String message,
          ClientRequestHandlerInterface handler, boolean remoteLookup) throws FailedDBOperationException {
    NSResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(guid, GROUPS,
            reader, signature, message, handler.getApp());
    if (errorCode.isAnError()) {
      return new ResultValue();
    }
    return NSFieldAccess.lookupListFieldAnywhere(guid, GROUPS, true, handler.getApp().getDB());
  }

  public static ResultValue lookupGroupsLocally(String guid, String reader, String signature, String message,
          ClientRequestHandlerInterface handler) {
    NSResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(guid, GROUPS,
            reader, signature, message, handler.getApp());
    if (errorCode.isAnError()) {
      return new ResultValue();
    }
    return NSFieldAccess.lookupListFieldLocallyNoAuth(guid, GROUPS, handler.getApp().getDB());
  }

  /**
   * Removes all group links when we're deleting a guid.
   *
   * @param guid
   * @param handler
   * @throws edu.umass.cs.gnscommon.exceptions.client.GnsClientException
   * @throws java.io.IOException
   * @throws org.json.JSONException
   */
  public static void cleanupGroupsForDelete(String guid, ClientRequestHandlerInterface handler)
          throws GnsClientException, IOException, JSONException {
    // just so you know all the nulls mean we're ignoring signatures and authentication
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("DELETE CLEANUP: " + guid);
    }
    try {
      for (String groupGuid : GroupAccess.lookupGroupsAnywhere(guid, null, null, null, handler, true).toStringSet()) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().info("GROUP CLEANUP: " + groupGuid);
        }
        removeFromGroup(groupGuid, guid, null, null, null, handler);
      }
    } catch (FailedDBOperationException e) {
      GNS.getLogger().severe("Unabled to remove guid from groups:" + e);
    }
  }

}
