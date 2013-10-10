package edu.umass.cs.gns.client;

import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.main.GNS;
import java.util.Set;

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
   * HIdden field that stores group members
   */
  public static final String GROUP = GNS.makeInternalField("group");
  /**
   * Hidden field that stores group member requests
   */
  public static final String GROUPREQUESTS = GNS.makeInternalField("groupRequests");

  public GroupAccess() {
  }

  // make it a singleton class
  public static GroupAccess getInstance() {
    return GroupAccessHolder.INSTANCE;
  }

  private static class GroupAccessHolder {

    private static final GroupAccess INSTANCE = new GroupAccess();
  }

  public boolean addToGroup(String guid, String memberGuid) {
    Intercessor client = Intercessor.getInstance();
    return client.sendUpdateRecordWithConfirmation(guid, GROUP, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public boolean removeFromGroup(String guid, String memberGuid) {
    Intercessor client = Intercessor.getInstance();
    return client.sendUpdateRecordWithConfirmation(guid, GROUP, memberGuid, null, UpdateOperation.REMOVE);
  }

  public ResultValue lookup(String guid) {
    Intercessor client = Intercessor.getInstance();
    ResultValue result = client.sendQuery(guid, GROUP);
    if (result != null) {
      return new ResultValue(result);
    } else {
      return new ResultValue();
    }
  }

  public boolean requestGroupAdmission(String guid, String memberGuid) {
    Intercessor client = Intercessor.getInstance();
    return client.sendUpdateRecordWithConfirmation(guid, GROUPREQUESTS, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public ResultValue retrieveGroupAdmissionRequests(String guid, String memberGuid) {
    Intercessor client = Intercessor.getInstance();
    ResultValue result = client.sendQuery(guid, GROUPREQUESTS);
    if (result != null) {
      return new ResultValue(result);
    } else {
      return new ResultValue();
    }
  }

  public boolean aprooveGroupAdmission(String guid, Set<String> requests) {
    Intercessor client = Intercessor.getInstance();
    ResultValue fieldValue = new ResultValue(requests);

    if (client.sendUpdateRecordWithConfirmation(guid, GROUP, fieldValue, null, UpdateOperation.APPEND_OR_CREATE)) {
      if (client.sendUpdateRecordWithConfirmation(guid, GROUPREQUESTS, fieldValue, null, UpdateOperation.REMOVE)) {
        return true;
      }
    }
    return false;
  }
  public static String Version = "$Revision$";
}
