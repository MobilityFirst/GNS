package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.clientsupport.InternalField;
import edu.umass.cs.gns.clientsupport.SelectHandler;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.util.ValuesMap;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
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

  public static final String GROUP_RECORDS = InternalField.makeInternalFieldString("groupRecords");
  public static final String GROUP_MIN_REFRESH_INTERVAL = InternalField.makeInternalFieldString("groupMinRefresh");
  public static final String GROUP_LAST_UPDATE = InternalField.makeInternalFieldString("groupLastUpdate");
  public static final String GROUP_QUERY_STRING = InternalField.makeInternalFieldString("groupQueryString");

  public static void updateMembers(String guid, Set<String> members, GnsReconfigurable activeReplica, InetSocketAddress lnsAddress) {
    NSResponseCode groupResponse = LNSUpdateHandler.sendUpdate(guid, GroupAccess.GROUP, new ResultValue(members),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
    // We could roll back the above operation if the one below gets an error, but we don't
    // We'll worry about this when we get transactions working.
    if (!groupResponse.isAnError()) {
      // This is probably a bad idea to update every member
      for (String member : members) {
        LNSUpdateHandler.sendUpdate(member, GroupAccess.GROUPS, new ResultValue(Arrays.asList(guid)),
                UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, activeReplica, lnsAddress);
      }
    }
  }

  /**
   * Return the members of a the group guid.
   *
   * @param guid
   * @param allowQueryToOtherNSs
   * @param activeReplica
   * @param lnsAddress
   * @return
   * @throws FailedDBOperationException
   */
  public static ResultValue lookupMembers(String guid, boolean allowQueryToOtherNSs, GnsReconfigurable activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    return NSFieldAccess.lookupListFieldAnywhere(guid, GroupAccess.GROUP, allowQueryToOtherNSs, activeReplica, lnsAddress);
  }

  public static boolean isGroupGuid(String guid, GnsReconfigurable activeReplica) throws FailedDBOperationException {
    return !NSFieldAccess.lookupListFieldOnThisServer(guid, GroupAccess.GROUP, activeReplica).isEmpty();
  }

  /**
   * Returns the groups that a GUID is a member of.
   *
   * @param guid
   * @param activeReplica
   * @return
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public static Set<String> lookupGroups(String guid, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupListFieldAnywhere(guid, GroupAccess.GROUPS, true, activeReplica, lnsAddress).toStringSet();
  }

  /**
   * Removes from the groupGuid the memberGuid.
   *
   * @param groupGuid
   * @param memberGuid
   * @param activeReplica
   * @return
   */
  public static NSResponseCode removeFromGroup(String groupGuid, String memberGuid, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) {
    NSResponseCode groupResponse = LNSUpdateHandler.sendUpdate(groupGuid, GroupAccess.GROUP,
            new ResultValue(Arrays.asList(memberGuid)),
            UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica, lnsAddress);
    // We could roll back the above operation if the one below gets an error, but we don't
    // We'll worry about this when we get transactions working.
    if (!groupResponse.isAnError()) {
      LNSUpdateHandler.sendUpdate(memberGuid, GroupAccess.GROUPS, new ResultValue(Arrays.asList(groupGuid)),
              UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica, lnsAddress);
    }
    return groupResponse;
  }

  /**
   * Removes all group links when we're deleting a guid.
   *
   * @param guid
   */
  public static void cleanupGroupsForDelete(String guid, GnsReconfigurableInterface activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    for (String groupGuid : lookupGroups(guid, activeReplica, lnsAddress)) {
      removeFromGroup(groupGuid, guid, activeReplica, lnsAddress);
    }
  }

  ///
  /// Support code for context sensitive group guids
  ///
  public static void updateLastUpdate(String guid, Date lastUpdate, GnsReconfigurable activeReplica, InetSocketAddress lnsAddress) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_LAST_UPDATE, new ResultValue(Arrays.asList(Long.toString(lastUpdate.getTime()))),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
  }

  public static void updateMinRefresh(String guid, int minRefresh, GnsReconfigurable activeReplica, InetSocketAddress lnsAddress) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, new ResultValue(Arrays.asList(Integer.toString(minRefresh))),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
  }

  public static void updateQueryString(String guid, String queryString, GnsReconfigurable activeReplica,
          InetSocketAddress lnsAddress) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_QUERY_STRING, new ResultValue(Arrays.asList(queryString)),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica, lnsAddress);
  }

  public static Date getLastUpdate(String guid, GnsReconfigurable activeReplica, InetSocketAddress lnsAddress)
          throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupListFieldAnywhere(guid, GROUP_LAST_UPDATE, true, activeReplica, lnsAddress);
    GNS.getLogger().fine("++++ResultValue = " + resultValue);
    if (!resultValue.isEmpty()) {
      return new Date(Long.parseLong((String) resultValue.get(0)));
    } else {
      return null;
    }
  }

  public static int getMinRefresh(String guid, GnsReconfigurable activeReplica, InetSocketAddress lnsAddress)
          throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupListFieldAnywhere(guid, GROUP_MIN_REFRESH_INTERVAL, true, activeReplica, lnsAddress);
    GNS.getLogger().fine("++++ResultValue = " + resultValue);
    if (!resultValue.isEmpty()) {
      return Integer.parseInt((String) resultValue.get(0));
    } else {
      // if we can't get it just return the default. No harm, no foul.
      return SelectHandler.DEFAULT_MIN_REFRESH_INTERVAL;
    }
  }

  public static String getQueryString(String guid, GnsReconfigurable activeReplica, InetSocketAddress lnsAddress)
          throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupListFieldAnywhere(guid, GROUP_QUERY_STRING, true, activeReplica, lnsAddress);
    GNS.getLogger().fine("++++ResultValue = " + resultValue);
    if (!resultValue.isEmpty()) {
      return (String) resultValue.get(0);
    } else {
      return null;
    }
  }

  public static ValuesMap lookupFieldInGroupGuid(String groupGuid, String field, GnsReconfigurable activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException, JSONException {
    JSONArray resultArray = new JSONArray();
    for (Object guidObject : lookupMembers(groupGuid, false, activeReplica, lnsAddress)) {
      String guid = (String) guidObject;
      ValuesMap valuesMap = NSFieldAccess.lookupFieldLocalAndRemote(guid, field, activeReplica, lnsAddress);
      if (valuesMap.has(field)) {
        resultArray.put(valuesMap.get(field));
      }
    }
    GNS.getLogger().info("Group result for " + groupGuid + "/" + field + " = " + resultArray.toString());
    ValuesMap result = new ValuesMap();
    result.put(field, resultArray);
    return result;
  }
}
