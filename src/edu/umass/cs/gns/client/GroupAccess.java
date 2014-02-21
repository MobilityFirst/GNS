package edu.umass.cs.gns.client;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.ResultValue;

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

  public static boolean addToGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, GROUP, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }
  
  public static boolean addToGroup(String guid, ResultValue members) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, GROUP, members, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public static boolean removeFromGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, GROUP, memberGuid, null, UpdateOperation.REMOVE);
  }
  
  public static boolean removeFromGroup(String guid, ResultValue members) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, GROUP, members, null, UpdateOperation.REMOVE);
  }

  public static ResultValue lookup(String guid) {
    
    ResultValue result = Intercessor.sendQuery(guid, GROUP, null, null, null);
    if (result != null) {
      return new ResultValue(result);
    } else {
      return new ResultValue();
    }
  }

  public static boolean requestJoinGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, JOINREQUESTS, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }
  
  public static boolean requestLeaveGroup(String guid, String memberGuid) {
    
    return Intercessor.sendUpdateRecordWithConfirmation(guid, LEAVEREQUESTS, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public static ResultValue retrieveGroupJoinRequests(String guid) {
    
    ResultValue result = Intercessor.sendQuery(guid, JOINREQUESTS, null, null, null);
    if (result != null) {
      return new ResultValue(result);
    } else {
      return new ResultValue();
    }
  }
  
  public static ResultValue retrieveGroupLeaveRequests(String guid) {
    
    ResultValue result = Intercessor.sendQuery(guid, LEAVEREQUESTS, null, null, null);
    if (result != null) {
      return new ResultValue(result);
    } else {
      return new ResultValue();
    }
  }

  public static boolean grantMembership(String guid, ResultValue requests) {
    

    if (Intercessor.sendUpdateRecordWithConfirmation(guid, GROUP, requests, null, UpdateOperation.APPEND_OR_CREATE)) {
      if (Intercessor.sendUpdateRecordWithConfirmation(guid, JOINREQUESTS, requests, null, UpdateOperation.REMOVE)) {
        return true;
      }
    }
    return false;
  }
  
  public static boolean revokeMembership(String guid, ResultValue requests) {
    

    if (Intercessor.sendUpdateRecordWithConfirmation(guid, GROUP, requests, null, UpdateOperation.REMOVE)) {
      if (Intercessor.sendUpdateRecordWithConfirmation(guid, LEAVEREQUESTS, requests, null, UpdateOperation.REMOVE)) {
        return true;
      }
    }
    return false;
  }
  public static String Version = "$Revision$";
}
