
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import com.google.common.collect.Sets;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.gnsapp.GNSCommandInternal;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSFieldAccess;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import edu.umass.cs.gnsserver.utils.JSONUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

//import edu.umass.cs.gnsserver.packet.QueryResultValue;

public class GroupAccess {

  // DONT FORGET TO CHECK THE CommandCategorys of the group commands
  // before you enable the new update methods.
  //private static final boolean USE_OLD_UPDATE = false;

  public static final String GROUP = InternalField.makeInternalFieldString("group");

  public static final String GROUPS = InternalField.makeInternalFieldString("groups");

  private final static Logger LOGGER = Logger.getLogger(GroupAccess.class.getName());


  public static ResponseCode addToGroup(InternalRequestHeader header, String groupGuid, String memberGuid, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler)
          throws IOException, JSONException, ClientException, InternalRequestException {

    // We need to update the members of the group and the groups of the member. 
    boolean membersUpdateOK = membersUpdateForAdd(header, groupGuid, Sets.newHashSet(memberGuid), handler);
    boolean groupsUpdateOK = groupsUpdateForAdd(header, groupGuid, memberGuid, handler);
    // If both updates were successfull we return success, otherwise not.
    if (membersUpdateOK && groupsUpdateOK) {
      return ResponseCode.NO_ERROR;
    } else {
      return ResponseCode.UPDATE_ERROR;
    }
  }


  public static ResponseCode addToGroup(InternalRequestHeader header, String groupGuid, ResultValue members, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {

    boolean membersUpdateOK = membersUpdateForAdd(header, groupGuid, members.toStringSet(), handler);
    boolean allGroupsUpdatesOK = true;
    for (String memberGuid : members.toStringSet()) {
      if (!groupsUpdateForAdd(header, groupGuid, memberGuid, handler)) {
        allGroupsUpdatesOK = false;
      }
    }
    if (membersUpdateOK && allGroupsUpdatesOK) {
      return ResponseCode.NO_ERROR;
    } else {
      return ResponseCode.UPDATE_ERROR;
    }
  }


  public static ResponseCode removeFromGroup(InternalRequestHeader header, CommandPacket commandPacket,
          String groupGuid, String memberGuid, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException, InternalRequestException {

    boolean membersUpdateOK = GNSProtocol.OK_RESPONSE.toString().equals(
            handler.getInternalClient().execute(GNSCommandInternal.fieldRemove(groupGuid,
                    GroupAccess.GROUP, memberGuid, header)).getResultString());
    boolean groupsUpdateOK = GNSProtocol.OK_RESPONSE.toString().equals(
            handler.getInternalClient().execute(GNSCommandInternal.fieldRemove(memberGuid,
                    GroupAccess.GROUPS, groupGuid, header)).getResultString());
    if (membersUpdateOK && groupsUpdateOK) {
      return ResponseCode.NO_ERROR;
    } else {
      return ResponseCode.UPDATE_ERROR;
    }
  }


  public static ResponseCode removeFromGroup(InternalRequestHeader header,
          CommandPacket commandPacket,
          String guid, ResultValue members, String writer,
          String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) throws ClientException, IOException, JSONException,
          InternalRequestException {
    boolean membersUpdateOK = GNSProtocol.OK_RESPONSE.toString().equals(
            handler.getInternalClient().execute(GNSCommandInternal.fieldRemoveList(guid,
                    GroupAccess.GROUP, members, header)).getResultString());
    boolean allGroupsUpdatesOK = true;
    for (String memberGuid : members.toStringSet()) {
      //handler.getRemoteQuery().fieldRemove(memberGuid, GroupAccess.GROUPS, guid);
      if (!GNSProtocol.OK_RESPONSE.toString().equals(handler.getInternalClient().execute(GNSCommandInternal.fieldRemove(memberGuid,
              GroupAccess.GROUPS, guid, header)).getResultString())) {
        allGroupsUpdatesOK = false;
      }
    }
    if (membersUpdateOK && allGroupsUpdatesOK) {
      return ResponseCode.NO_ERROR;
    } else {
      return ResponseCode.UPDATE_ERROR;
    }
  }


  public static ResultValue lookup(InternalRequestHeader header, CommandPacket commandPacket,
          String guid, String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, commandPacket,
            guid, GROUP,
            null, //fields
            reader, signature, message, timestamp,
            handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new ResultValue();
    }
    return NSFieldAccess.lookupListFieldLocallySafe(guid, GROUP, handler.getApp().getDB());
  }


  public static ResultValue lookupGroupsAnywhere(InternalRequestHeader header, CommandPacket commandPacket,
          String guid, String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler, boolean remoteLookup) throws FailedDBOperationException {
    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, commandPacket, guid,
            GROUPS,
            null, // fields
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new ResultValue();
    }
    return NSFieldAccess.lookupListFieldAnywhere(header, guid, GROUPS, true, handler);
  }


  public static ResultValue lookupGroupsLocally(InternalRequestHeader header, CommandPacket commandPacket,
          String guid, String reader, String signature, String message, Date timestamp,
          ClientRequestHandlerInterface handler) {
    ResponseCode errorCode = FieldAccess.signatureAndACLCheckForRead(header, commandPacket, guid, GROUPS,
            null, //fields
            reader, signature, message, timestamp, handler.getApp());
    if (errorCode.isExceptionOrError()) {
      return new ResultValue();
    }
    return NSFieldAccess.lookupListFieldLocallySafe(guid, GROUPS, handler.getApp().getDB());
  }

  //
  // Helper methods
  //
  private static boolean membersUpdateForAdd(InternalRequestHeader header, String groupGuid, Set<String> newMembers,
          ClientRequestHandlerInterface handler)
          throws IOException, JSONException, ClientException, InternalRequestException {
    // We need to update the members of the group.
    // We need to do this in a way that
    // multiple invocations of this command result in the same 
    // values in the distributed database.
    // First init some things to keep track of what's happened.
    String result = GNSProtocol.OK_RESPONSE.toString();
    Set<String> currentMembers;
    // Find which new members are not in the group
    try {
      currentMembers
              = JSONUtils.JSONArrayToHashSet(handler.getInternalClient().execute(GNSCommandInternal.
                      fieldRead(groupGuid, GROUP, header)).getResultJSONArray());
      newMembers.removeAll(currentMembers);
    } catch (JSONException | InternalRequestException | IOException | ClientException e) {
      result = "LOOKUP_ERROR";
    }
    // If there are any new members not in the group we add the here
    if (!newMembers.isEmpty()) {
      result = handler.getInternalClient().execute(
              GNSCommandInternal.fieldUpdate(header,
                      CommandType.AppendOrCreateListUnsigned,
                      GNSProtocol.GUID.toString(), groupGuid,
                      GNSProtocol.FIELD.toString(), GROUP,
                      GNSProtocol.VALUE.toString(), newMembers
              )).getResultString();
    }
    return GNSProtocol.OK_RESPONSE.toString().equals(result);
  }

  private static boolean groupsUpdateForAdd(InternalRequestHeader header, String groupGuid, String memberGuid,
          ClientRequestHandlerInterface handler)
          throws IOException, JSONException, ClientException, InternalRequestException {
    // We need to update the groups of the member. 
    // We need to do this in a way that
    // multiple invocations of this command result in the same 
    // values in the distributed database.
    // First init some things to keep track of what's happened.
    String result = GNSProtocol.OK_RESPONSE.toString();
    boolean foundInGroups = false;
    // If the group is already in groups of the member we don't need to do the update.
    try {
      foundInGroups = JSONUtils.JSONArrayContains(groupGuid,
              handler.getInternalClient().execute(GNSCommandInternal.
                      fieldRead(memberGuid, GROUPS, header)).getResultJSONArray());
    } catch (JSONException | InternalRequestException | IOException | ClientException e) {
      result = "LOOKUP_ERROR";
    }
    if (!foundInGroups) {
      result = handler.getInternalClient().execute(
              GNSCommandInternal.fieldUpdate(header,
                      CommandType.AppendOrCreateListUnsigned,
                      GNSProtocol.GUID.toString(), memberGuid,
                      GNSProtocol.FIELD.toString(), GROUPS,
                      GNSProtocol.VALUE.toString(), new ResultValue(Arrays.asList(groupGuid))
              )).getResultString();
    }
    return GNSProtocol.OK_RESPONSE.toString().equals(result);
  }


  public static void cleanupGroupsForDelete(InternalRequestHeader header, CommandPacket commandPacket,
          String guid, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException,
          InternalRequestException {

    LOGGER.log(Level.FINE, "OLD DELETE CLEANUP: {0}", guid);
    try {
      // We're ignoring signatures and authentication
      for (String groupGuid : GroupAccess.lookupGroupsAnywhere(header, commandPacket, guid,
              GNSProtocol.INTERNAL_QUERIER.toString(),
              //GNSConfig.getInternalOpSecret(),
              null, null,
              null, handler, true).toStringSet()) {
        LOGGER.log(Level.FINE, "OLD GROUP CLEANUP: {0}", groupGuid);
        removeFromGroup(header, commandPacket, groupGuid, guid,
                GNSProtocol.INTERNAL_QUERIER.toString(),
                //GNSConfig.getInternalOpSecret(),
                null, null, null,
                handler);
      }
    } catch (FailedDBOperationException e) {
      LOGGER.log(Level.SEVERE, "Unabled to remove guid from groups:{0}", e);
    }
  }


  public static ResponseCode removeGuidFromGroups(InternalRequestHeader header, CommandPacket commandPacket,
          String memberGuid, ClientRequestHandlerInterface handler)
          throws ClientException, IOException, JSONException,
          InternalRequestException {

    LOGGER.log(Level.FINE, "DELETE CLEANUP: {0}", memberGuid);
    try {
      boolean allUpdatesOK = true;
      // We're ignoring signatures and authentication
      for (String groupGuid : GroupAccess.lookupGroupsAnywhere(header, commandPacket, memberGuid,
              GNSProtocol.INTERNAL_QUERIER.toString(),
              null, null,
              null, handler, true).toStringSet()) {
        LOGGER.log(Level.FINE, "GROUP CLEANUP: {0}", groupGuid);
        if (!GNSProtocol.OK_RESPONSE.toString().equals(
                handler.getInternalClient().execute(GNSCommandInternal.fieldRemove(groupGuid,
                        GroupAccess.GROUP, memberGuid, header)).getResultString())) {
          allUpdatesOK = false;
        }
      }
      if (allUpdatesOK) {
        return ResponseCode.NO_ERROR;
      } else {
        return ResponseCode.UPDATE_ERROR;
      }
    } catch (FailedDBOperationException e) {
      LOGGER.log(Level.SEVERE, "Unabled to remove guid from groups:{0}", e);
      return ResponseCode.UPDATE_ERROR;
    }
  }

}
