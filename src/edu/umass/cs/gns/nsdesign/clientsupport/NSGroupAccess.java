package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.clientsupport.InternalField;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;

//import edu.umass.cs.gns.packet.QueryResultValue;
/**
 * * DO NOT not use any class in package edu.umass.cs.gns.nsdesign **
 */
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

  public static ResultValue lookupMembers(String guid, boolean allowQueryToOtherNSs, GnsReconfigurable activeReplica) {
    return NSFieldAccess.lookupField(guid, GroupAccess.GROUP, allowQueryToOtherNSs, activeReplica);
  }
  
//  public static ResultValue lookupGroupRecords(String guid, boolean allowQueryToOtherNSs, GnsReconfigurable activeReplica) {
//    return NSFieldAccess.lookupField(guid, GROUP_RECORDS, allowQueryToOtherNSs, activeReplica);
//  }

  public static void updateMembers(String guid, Set<String> members, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GroupAccess.GROUP, new ResultValue(members),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

//  public static void updateRecords(String guid, JSONArray records, GnsReconfigurable activeReplica) throws JSONException {
//    LNSUpdateHandler.sendUpdate(guid, GROUP_RECORDS, new ResultValue(records.toString()),
//            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
//  }

  public static void updateLastUpdate(String guid, Date lastUpdate, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_LAST_UPDATE, new ResultValue(Arrays.asList(Long.toString(lastUpdate.getTime()))),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static void updateMinRefresh(String guid, int minRefresh, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, new ResultValue(Arrays.asList(Integer.toString(minRefresh))),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }
  
  public static void updateQueryString(String guid, String queryString, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_QUERY_STRING, new ResultValue(Arrays.asList(queryString)),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static Date getLastUpdate(String guid, GnsReconfigurable activeReplica) {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_LAST_UPDATE, true, activeReplica);
    GNS.getLogger().info("++++ResultValue = " +resultValue);
    if (!resultValue.isEmpty()) {
      return new Date(Long.parseLong((String) resultValue.get(0)));
    } else {
      return null;
    }
  }

  public static int getMinRefresh(String guid, GnsReconfigurable activeReplica) {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_MIN_REFRESH_INTERVAL, true, activeReplica);
    GNS.getLogger().info("++++ResultValue = " +resultValue);
    if (!resultValue.isEmpty()) {
      return Integer.parseInt((String) resultValue.get(0));
    } else {
      return 0;
    }
  }
  
  public static String getQueryString(String guid, GnsReconfigurable activeReplica) {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_QUERY_STRING, true, activeReplica);
    GNS.getLogger().info("++++ResultValue = " +resultValue);
    if (!resultValue.isEmpty()) {
      return (String) resultValue.get(0);
    } else {
      return null;
    }
  }

}
