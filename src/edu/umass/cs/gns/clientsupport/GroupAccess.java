package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.NSResponseCode;

//import edu.umass.cs.gns.packet.QueryResultValue;
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
   * Hidden field that stores group member join requests
   */
  @Deprecated
  public static final String JOINREQUESTS = InternalField.makeInternalFieldString("groupJoinRequests");
  /**
   * Hidden field that stores group member quit requests
   */
   @Deprecated
  public static final String LEAVEREQUESTS = InternalField.makeInternalFieldString("groupLeaveRequests");

  /**
   * Sends a request to the NS to add a single GUID to a group.
   *
   * @param guid
   * @param memberGuid
   * @param writer
   * @param signature
   * @param message
   * @return
   */
  public static NSResponseCode addToGroup(String guid, String memberGuid, String writer, String signature, String message) {

    NSResponseCode groupResponse = LocalNameServer.getIntercessor().sendUpdateRecord(guid, GROUP, memberGuid, null, 1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message);
    // We could roll back the above operation if the one below gets an error, but we don't
    // We'll worry about that when we migrate this into the Name Server
    if (!groupResponse.isAnError()) {
      LocalNameServer.getIntercessor().sendUpdateRecordBypassingAuthentication(memberGuid, GROUPS, guid, null,
              UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
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
   * @return
   */
  public static NSResponseCode addToGroup(String guid, ResultValue members, String writer, String signature, String message) {
    NSResponseCode groupResponse = LocalNameServer.getIntercessor().sendUpdateRecord(guid, GROUP, members, null, 1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message);
    if (!groupResponse.isAnError()) {
      // We could fix the above operation if any one below gets an error, but we don't
      // We'll worry about that when we migrate this into the Name Server
      for (String memberGuid : members.toStringSet()) {
        LocalNameServer.getIntercessor().sendUpdateRecordBypassingAuthentication(memberGuid, GROUPS, guid, null,
                UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
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
   * @return
   */
  public static NSResponseCode removeFromGroup(String guid, String memberGuid, String writer, String signature, String message) {
    NSResponseCode groupResponse = LocalNameServer.getIntercessor().sendUpdateRecord(guid, GROUP, memberGuid, null, 1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message);
    // We could roll back the above operation if the one below gets an error, but we don't
    // We'll worry about that when we migrate this into the Name Server
    if (!groupResponse.isAnError()) {
      LocalNameServer.getIntercessor().sendUpdateRecordBypassingAuthentication(memberGuid, GROUPS, guid, null,
              UpdateOperation.SINGLE_FIELD_REMOVE);
    }
    return groupResponse;
  }

  /**
   * Sends a request to the NS to remove a list of GUIDs from a group.
   *
   * @param guid
   * @param members
   * @param writer
   * @param signature
   * @param message
   * @return
   */
  public static NSResponseCode removeFromGroup(String guid, ResultValue members, String writer, String signature, String message) {
    NSResponseCode groupResponse = LocalNameServer.getIntercessor().sendUpdateRecord(guid, GROUP, members, null, 1,
            UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message);
    if (!groupResponse.isAnError()) {
      // We could fix the above operation if any one below gets an error, but we don't
      // We'll worry about that when we migrate this into the Name Server
      for (String memberGuid : members.toStringSet()) {
        LocalNameServer.getIntercessor().sendUpdateRecordBypassingAuthentication(memberGuid, GROUPS, guid, null,
                UpdateOperation.SINGLE_FIELD_REMOVE);
      }
    }
    return groupResponse;
  }

  /**
   * Returns the members of the group GUID.
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @return
   */
  public static ResultValue lookup(String guid, String reader, String signature, String message) {
    QueryResult result = LocalNameServer.getIntercessor().sendQuery(guid, GROUP, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (!result.isError()) {
      return new ResultValue(result.getArray(GROUP));
    } else {
      return new ResultValue();
    }
  }

  /**
   * Returns the groups that a GUID is a member of.
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @return
   */
  public static ResultValue lookupGroups(String guid, String reader, String signature, String message) {
    QueryResult result = LocalNameServer.getIntercessor().sendQuery(guid, GROUPS, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (!result.isError()) {
      return new ResultValue(result.getArray(GROUPS));
    } else {
      return new ResultValue();
    }
  }

  /**
   * Removes all group links when we're deleting a guid.
   * 
   * @param guid 
   */
  public static void cleanupGroupsForDelete(String guid) {
    // just so you know all the nulls mean we're ignoring signatures and authentication
    for (String groupGuid : GroupAccess.lookupGroups(guid, null, null, null).toStringSet()) {
      removeFromGroup(groupGuid, guid, null, null, null);
    }
  }

  /**
   *
   * @param guid
   * @param memberGuid
   * @param writer
   * @param signature
   * @param message
   * @return
   * @deprecated
   */
  @Deprecated
  public static NSResponseCode requestJoinGroup(String guid, String memberGuid, String writer, String signature, String message) {

    return LocalNameServer.getIntercessor().sendUpdateRecord(guid, JOINREQUESTS, memberGuid, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message);
  }

  /**
   *
   * @param guid
   * @param memberGuid
   * @param writer
   * @param signature
   * @param message
   * @return
   * @deprecated
   */
  @Deprecated
  public static NSResponseCode requestLeaveGroup(String guid, String memberGuid, String writer, String signature, String message) {

    return LocalNameServer.getIntercessor().sendUpdateRecord(guid, LEAVEREQUESTS, memberGuid, null, -1,
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message);
  }

  /**
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @return
   * @deprecated
   */
  @Deprecated
  public static ResultValue retrieveGroupJoinRequests(String guid, String reader, String signature, String message) {
    QueryResult result = LocalNameServer.getIntercessor().sendQuery(guid, JOINREQUESTS, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (!result.isError()) {
      return new ResultValue(result.getArray(JOINREQUESTS));
    } else {
      return new ResultValue();
    }
  }

  /**
   *
   * @param guid
   * @param reader
   * @param signature
   * @param message
   * @return
   * @deprecated
   */
  @Deprecated
  public static ResultValue retrieveGroupLeaveRequests(String guid, String reader, String signature, String message) {
    QueryResult result = LocalNameServer.getIntercessor().sendQuery(guid, LEAVEREQUESTS, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (!result.isError()) {
      return new ResultValue(result.getArray(LEAVEREQUESTS));
    } else {
      return new ResultValue();
    }
  }
  
  /**
   *
   * @param guid
   * @param requests
   * @param writer
   * @param signature
   * @param message
   * @return
   * @deprecated
   */
  @Deprecated
  public static boolean grantMembership(String guid, ResultValue requests, String writer, String signature, String message) {

    if (!addToGroup(guid, requests, writer, signature, message).isAnError()) {
      //if (!LocalNameServer.getIntercessor().sendUpdateRecord(guid, GROUP, requests, null, UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, writer, signature, message).isAnError()) {
      if (!LocalNameServer.getIntercessor().sendUpdateRecord(guid, JOINREQUESTS, requests, null, -1,
              UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message).isAnError()) {
        return true;
      }
    }
    return false;
  }

  /**
   *
   * @param guid
   * @param requests
   * @param writer
   * @param signature
   * @param message
   * @return
   * @deprecated
   */
  @Deprecated
  public static boolean revokeMembership(String guid, ResultValue requests, String writer, String signature, String message) {

    if (!removeFromGroup(guid, requests, writer, signature, message).isAnError()) {
      //if (!LocalNameServer.getIntercessor().sendUpdateRecord(guid, GROUP, requests, null, UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message).isAnError()) {
      if (!LocalNameServer.getIntercessor().sendUpdateRecord(guid, LEAVEREQUESTS, requests, null, -1,
              UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message).isAnError()) {
        return true;
      }
    }
    return false;
  }

  /**
   *
   */
  public static String Version = "$Revision$";
}
