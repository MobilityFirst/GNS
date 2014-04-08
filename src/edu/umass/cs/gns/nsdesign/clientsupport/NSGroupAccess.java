package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.AccountAccess;
import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.clientsupport.InternalField;
import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import org.json.JSONObject;

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

  public static ResultValue lookup(String guid, boolean allowQueryToOtherNSs, GnsReconfigurable activeReplica) {
    return NSFieldAccess.lookupField(guid, GroupAccess.GROUP, allowQueryToOtherNSs, activeReplica);
  }

  public static void updateMembers(String guid, Set<String> members, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GroupAccess.GROUP, new ResultValue(members),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

  // NOT SURE HOW TO DO THIS YET
  public static void updateRecords(String guid, Set<JSONObject> records, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_RECORDS, new ResultValue(records),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static void updateLastUpdate(String guid, Date lastUpdate, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_LAST_UPDATE, new ResultValue(Arrays.asList(Long.toString(lastUpdate.getTime()))),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static void updateMinRefresh(String guid, int minRefresh, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_MIN_REFRESH_INTERVAL, new ResultValue(Arrays.asList(Integer.toString(minRefresh))),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static Date getLastUpdate(String guid, GnsReconfigurable activeReplica) {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_LAST_UPDATE, true, activeReplica);
    if (!resultValue.isEmpty()) {
      return new Date(Long.parseLong((String) resultValue.get(0)));
    } else {
      return null;
    }
  }

  public static int getMinRefresh(String guid, GnsReconfigurable activeReplica) {
    ResultValue resultValue = NSFieldAccess.lookupField(guid, GROUP_MIN_REFRESH_INTERVAL, true, activeReplica);
    if (!resultValue.isEmpty()) {
      return Integer.parseInt((String) resultValue.get(0));
    } else {
      return 0;
    }
  }

}
