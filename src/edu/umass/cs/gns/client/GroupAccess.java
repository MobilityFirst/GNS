package edu.umass.cs.gns.client;

import edu.umass.cs.gns.packet.UpdateOperation;

import java.util.ArrayList;

//import edu.umass.cs.gns.packet.QueryResultValue;

/**
 * GroupAccess provides an interface to the group information in the GNSR.
 *
 * The members of a group are stored in a record whose key is the GROUPV2 string. 
 *
 * @author westy
 */
public class GroupAccess {

    public GroupAccess() {
    }

    // make it a singleton class
    public static GroupAccess getInstance() {
        return GroupAccessHolder.INSTANCE;
    }

    private static class GroupAccessHolder {

        private static final GroupAccess INSTANCE = new GroupAccess();
    }
    public static final String GROUP = Defs.INTERNAL_PREFIX + "group";

    public boolean addToGroup(String guid, String memberGuid) {
        Intercessor client = Intercessor.getInstance();
        return client.sendUpdateRecordWithConfirmation(guid, GROUP, memberGuid, null, UpdateOperation.APPEND_OR_CREATE);
    }

    public boolean removeFromGroup(String guid, String memberGuid) {
        Intercessor client = Intercessor.getInstance();
        return client.sendUpdateRecordWithConfirmation(guid, GROUP, memberGuid, null, UpdateOperation.REMOVE);
    }

    public ArrayList<String> lookup(String guid) {
        Intercessor client = Intercessor.getInstance();
      ArrayList<String> result = client.sendQuery(guid, GROUP);
        if (result != null) {
            return new ArrayList<String>(result);
        } else {
            return new ArrayList<String>();
        }
    }
    public static String Version = "$Revision: 645 $";
}
