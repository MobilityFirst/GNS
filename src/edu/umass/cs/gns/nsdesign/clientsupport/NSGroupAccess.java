package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.clientsupport.InternalField;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

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

  public static ResultValue lookupMembers(String guid, boolean allowQueryToOtherNSs, GnsReconfigurable activeReplica) throws FailedDBOperationException {
    return NSFieldAccess.lookupField(guid, GroupAccess.GROUP, allowQueryToOtherNSs, activeReplica);
  }
  
  public static void updateMembers(String guid, Set<String> members, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GroupAccess.GROUP, new ResultValue(members),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica);
  }
  
  /**
   * Returns the groups that a GUID is a member of.
   *
   * @param guid
   * @return
   */
  public static Set<String> lookupGroups(String guid, GnsReconfigurableInterface activeReplica) throws FailedDBOperationException {
    // this guid could be on another NS hence the true below
    return NSFieldAccess.lookupField(guid, GroupAccess.GROUPS, true, activeReplica).toStringSet();
  }
  
  /**
   * Removes from the groupGuid the memberGuid.
   * 
   * @param groupGuid
   * @param memberGuid
   * @param activeReplica
   * @return 
   */
  public static NSResponseCode removeFromGroup(String groupGuid, String memberGuid, GnsReconfigurableInterface activeReplica) {
    NSResponseCode groupResponse =  LNSUpdateHandler.sendUpdate(groupGuid, GroupAccess.GROUP, new ResultValue(Arrays.asList(memberGuid)),
            UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica);
    // We could roll back the above operation if the one below gets an error, but we don't
    // We'll worry about this when we get transactions working.
    if (!groupResponse.isAnError()) {
       LNSUpdateHandler.sendUpdate(memberGuid, GroupAccess.GROUPS, new ResultValue(Arrays.asList(groupGuid)),
              UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica);
    }
    return groupResponse;
  }

/**
   * Removes all group links when we're deleting a guid.
   * 
   * @param guid 
   */
  public static void cleanupGroupsForDelete(String guid, GnsReconfigurableInterface activeReplica) throws FailedDBOperationException {
    for (String groupGuid : lookupGroups(guid, activeReplica)) {
      removeFromGroup(groupGuid, guid, activeReplica);
    }
  }

  public static void updateLastUpdate(String guid, Date lastUpdate, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_LAST_UPDATE, new ResultValue(Arrays.asList(Long.toString(lastUpdate.getTime()))),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static void updateMinRefresh(String guid, int minRefresh, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, new ResultValue(Arrays.asList(Integer.toString(minRefresh))),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica);
  }
  
  public static void updateQueryString(String guid, String queryString, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_QUERY_STRING, new ResultValue(Arrays.asList(queryString)),
            UpdateOperation.SINGLE_FIELD_REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static Date getLastUpdate(String guid, GnsReconfigurable activeReplica) throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_LAST_UPDATE, true, activeReplica);
    GNS.getLogger().fine("++++ResultValue = " +resultValue);
    if (!resultValue.isEmpty()) {
      return new Date(Long.parseLong((String) resultValue.get(0)));
    } else {
      return null;
    }
  }

  public static int getMinRefresh(String guid, GnsReconfigurable activeReplica) throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_MIN_REFRESH_INTERVAL, true, activeReplica);
    GNS.getLogger().fine("++++ResultValue = " +resultValue);
    if (!resultValue.isEmpty()) {
      return Integer.parseInt((String) resultValue.get(0));
    } else {
      return 0;
    }
  }
  
  public static String getQueryString(String guid, GnsReconfigurable activeReplica) throws FailedDBOperationException {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_QUERY_STRING, true, activeReplica);
    GNS.getLogger().fine("++++ResultValue = " +resultValue);
    if (!resultValue.isEmpty()) {
      return (String) resultValue.get(0);
    } else {
      return null;
    }
  }

}
