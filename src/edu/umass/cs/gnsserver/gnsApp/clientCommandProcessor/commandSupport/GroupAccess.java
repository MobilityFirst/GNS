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
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.main.GNS;
import java.io.IOException;
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
          ClientRequestHandlerInterface handler) {

    NSResponseCode groupResponse = FieldAccess.update(guid, GROUP, memberGuid, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message, handler);

//    handler.setReallySendUpdateToReplica(true);
//    NSResponseCode groupResponse = handler.getIntercessor().sendUpdateRecord(guid, GROUP, memberGuid, null, 1,
//            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message);
//    handler.setReallySendUpdateToReplica(false);
    // FIXME: We could roll back the above operation if the one below gets an error, but we don't
    // We'll worry about that when we migrate this into the Name Server
    if (!groupResponse.isAnError()) {
      FieldAccess.update(memberGuid, GROUPS, guid, null, -1,
              UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, null, null, null, handler);
//      handler.setReallySendUpdateToReplica(true);
//      handler.getIntercessor().sendUpdateRecordBypassingAuthentication(memberGuid, GROUPS, guid, null,
//              UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
//      handler.setReallySendUpdateToReplica(false);
    }
    return groupResponse;
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
          ClientRequestHandlerInterface handler) {
    NSResponseCode groupResponse = FieldAccess.update(guid, GROUP, members, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message, handler);

//    handler.setReallySendUpdateToReplica(true);
//    NSResponseCode groupResponse = handler.getIntercessor().sendUpdateRecord(guid, GROUP, members, null, 1,
//            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message, true);
//    handler.setReallySendUpdateToReplica(false);
    if (!groupResponse.isAnError()) {
      // FIXME: We could fix the above operation if any one below gets an error, but we don't
      // We'll worry about that when we migrate this into the Name Server
      for (String memberGuid : members.toStringSet()) {
        FieldAccess.update(memberGuid, GROUPS, guid, null, -1,
                UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message, handler);
//        handler.setReallySendUpdateToReplica(true);
//        handler.getIntercessor().sendUpdateRecordBypassingAuthentication(memberGuid, GROUPS, guid, null,
//                UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
//        handler.setReallySendUpdateToReplica(false);
      }
    }
    return groupResponse;
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
   */
  public static NSResponseCode removeFromGroup(String guid, String memberGuid, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {
    NSResponseCode response = FieldAccess.update(guid, GROUP, memberGuid, null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, handler);
    //handler.getRemoteQuery().fieldRemove(guid, GroupAccess.GROUP, memberGuid);
    if (response == NSResponseCode.NO_ERROR) {
      FieldAccess.update(memberGuid, GROUPS, guid, null, -1,
              UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, handler);
      //handler.getRemoteQuery().fieldRemove(memberGuid, GroupAccess.GROUPS, guid);
    } else {
      return response;
    }
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
   */
  public static NSResponseCode removeFromGroup(String guid, ResultValue members, String writer, String signature, String message,
          ClientRequestHandlerInterface handler) {

    NSResponseCode response = FieldAccess.update(guid, GROUP, members, null, -1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, handler);
    // handler.getRemoteQuery().fieldRemoveMultiple(guid, GroupAccess.GROUP, members);
    if (response == NSResponseCode.NO_ERROR) {
      for (String memberGuid : members.toStringSet()) {
        FieldAccess.update(memberGuid, GROUPS, guid, null, -1,
                UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, handler);
        //handler.getRemoteQuery().fieldRemove(memberGuid, GroupAccess.GROUPS, guid);
      }
    } else {
      return response;
    }
    return NSResponseCode.NO_ERROR;

//    handler.setReallySendUpdateToReplica(true);
//    NSResponseCode groupResponse = handler.getIntercessor().sendUpdateRecord(guid, GROUP, members, null, 1,
//            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message, true);
//    handler.setReallySendUpdateToReplica(false);
//    if (!groupResponse.isAnError()) {
//      // We could fix the above operation if any one below gets an error, but we don't
//      // We'll worry about that when we migrate this into the Name Server
//      for (String memberGuid : members.toStringSet()) {
//        handler.setReallySendUpdateToReplica(true);
//        handler.getIntercessor().sendUpdateRecordBypassingAuthentication(memberGuid, GROUPS, guid, null,
//                UpdateOperation.SINGLE_FIELD_REMOVE);
//        handler.setReallySendUpdateToReplica(false);
//      }
//    }
//    return groupResponse;
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
    // FIXME: handle authentication
    return NSFieldAccess.lookupListFieldLocallyNoAuth(guid, GROUP, handler.getApp().getDB());
//    QueryResult<String> result = handler.getIntercessor().sendSingleFieldQuery(guid, GROUP, reader, signature, message, ColumnFieldType.LIST_STRING);
//    if (!result.isError()) {
//      return new ResultValue(result.getArray(GROUP));
//    } else {
//      return new ResultValue();
//    }
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
  public static ResultValue lookupGroups(String guid, String reader, String signature, String message,
          ClientRequestHandlerInterface handler) {
    // FIXME: handle authentication
    return NSFieldAccess.lookupListFieldLocallyNoAuth(guid, GROUPS, handler.getApp().getDB());
//    QueryResult<String> result = handler.getIntercessor().sendSingleFieldQuery(guid, GROUPS, reader, signature, message, ColumnFieldType.LIST_STRING);
//    if (!result.isError()) {
//      return new ResultValue(result.getArray(GROUPS));
//    } else {
//      return new ResultValue();
//    }
  }

  /**
   * Removes all group links when we're deleting a guid.
   *
   * @param guid
   * @param handler
   */
  public static void cleanupGroupsForDelete(String guid, ClientRequestHandlerInterface handler) {
    // just so you know all the nulls mean we're ignoring signatures and authentication
    for (String groupGuid : GroupAccess.lookupGroups(guid, null, null, null, handler).toStringSet()) {
      removeFromGroup(groupGuid, guid, null, null, null, handler);
    }
  }

}
