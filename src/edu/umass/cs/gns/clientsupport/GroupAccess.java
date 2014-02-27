package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.packet.NSResponseCode;

//import edu.umass.cs.gns.packet.QueryResultValue;
/**
 * GroupAccess provides an interface to the group information in the GNSR.
 *
 * The members of a group are stored in a record whose key is the GROUP string. 
 *
 * @author westy
 */
public class GroupAccess {

  /**
   * Hidden field that stores group members
   */
  public static final String GROUP = GNS.makeInternalField("group");
  /**
   * Hidden field that stores group member join requests
   */
  public static final String JOINREQUESTS = GNS.makeInternalField("joinRequests");
  /**
   * Hidden field that stores group member quit requests
   */
  public static final String LEAVEREQUESTS = GNS.makeInternalField("leaveRequests");

  public static NSResponseCode addToGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordBypassingAuthentication(guid, GROUP, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }
  
  public static NSResponseCode addToGroup(String guid, ResultValue members) {
    
    return Intercessor.sendUpdateRecordBypassingAuthentication(guid, GROUP, members, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public static NSResponseCode removeFromGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordBypassingAuthentication(guid, GROUP, memberGuid, null, UpdateOperation.REMOVE);
  }
  
  public static NSResponseCode removeFromGroup(String guid, ResultValue members) {
    
    return Intercessor.sendUpdateRecordBypassingAuthentication(guid, GROUP, members, null, UpdateOperation.REMOVE);
  }

  public static ResultValue lookup(String guid) {
    
    QueryResult result = Intercessor.sendQueryBypassingAuthentication(guid, GROUP);
    if (!result.isError()) {
      return new ResultValue(result.get(GROUP));
    } else {
      return new ResultValue();
    }
  }

  public static NSResponseCode requestJoinGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordBypassingAuthentication(guid, JOINREQUESTS, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }
  
  public static NSResponseCode requestLeaveGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordBypassingAuthentication(guid, LEAVEREQUESTS, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public static ResultValue retrieveGroupJoinRequests(String guid) {
    QueryResult result = Intercessor.sendQueryBypassingAuthentication(guid, JOINREQUESTS);
    if (!result.isError()) {
      return new ResultValue(result.get(JOINREQUESTS));
    } else {
      return new ResultValue();
    }
  }
  
  public static ResultValue retrieveGroupLeaveRequests(String guid) {
    QueryResult result = Intercessor.sendQueryBypassingAuthentication(guid, LEAVEREQUESTS);
    if (!result.isError()) {
      return new ResultValue(result.get(LEAVEREQUESTS));
    } else {
      return new ResultValue();
    }
  }

  public static boolean grantMembership(String guid, ResultValue requests) {
    

    if (!Intercessor.sendUpdateRecordBypassingAuthentication(guid, GROUP, requests, null, UpdateOperation.APPEND_OR_CREATE).isAnError()) {
      if (!Intercessor.sendUpdateRecordBypassingAuthentication(guid, JOINREQUESTS, requests, null, UpdateOperation.REMOVE).isAnError()) {
        return true;
      }
    }
    return false;
  }
  
  public static boolean revokeMembership(String guid, ResultValue requests) {
    

    if (!Intercessor.sendUpdateRecordBypassingAuthentication(guid, GROUP, requests, null, UpdateOperation.REMOVE).isAnError()) {
      if (!Intercessor.sendUpdateRecordBypassingAuthentication(guid, LEAVEREQUESTS, requests, null, UpdateOperation.REMOVE).isAnError()) {
        return true;
      }
    }
    return false;
  }
  public static String Version = "$Revision$";
}
