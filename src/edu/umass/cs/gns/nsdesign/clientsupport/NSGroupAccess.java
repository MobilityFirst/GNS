package edu.umass.cs.gns.nsdesign.clientsupport;

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
  public static final String GROUP_MIN_REFRESH_INTERVAL = InternalField.makeInternalFieldString("minRefresh");
  public static final String GROUP_LAST_UPDATE = InternalField.makeInternalFieldString("lastUpdate");

  public static ResultValue lookup(String guid, GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException {
    NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, GroupAccess.GROUP);
    return nameRecord.getKey(GroupAccess.GROUP);
  }

  public static void updateMembers(String guid, Set<String> members, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GroupAccess.GROUP, new ResultValue(members),
            UpdateOperation.REPLACE_ALL_OR_CREATE, activeReplica);
  }

  public static void updateRecords(String guid, Set<JSONObject> members, GnsReconfigurable activeReplica) {
    LNSUpdateHandler.sendUpdate(guid, GROUP_RECORDS, new ResultValue(members),
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
    ResultValue resultValue = LNSQueryHandler.lookupField(guid, GROUP_LAST_UPDATE, activeReplica);
    if (resultValue != null && !resultValue.isEmpty()) {
      return new Date(Long.parseLong((String) resultValue.get(0)));
    } else {
      return null;
    }
  }

  public static int getMinRefresh(String guid, GnsReconfigurable activeReplica) {
    ResultValue resultValue = LNSQueryHandler.lookupField(guid, GROUP_MIN_REFRESH_INTERVAL, activeReplica);
    if (resultValue != null && !resultValue.isEmpty()) {
      return Integer.parseInt((String) resultValue.get(0));
    } else {
      return 0;
    }
  }

}
