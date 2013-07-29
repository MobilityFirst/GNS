package edu.umass.cs.gnrs.client;

import edu.umass.cs.gnrs.packet.QueryResultValue;
import edu.umass.cs.gnrs.packet.UpdateOperation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Document once it settles down a bit.
 *
 * @author westy
 */
public class AclAccess {

  public static String Version = "$Revision: 645 $";

  public AclAccess() {
  }

  public enum AccessType {

    READ_WHITELIST, WRITE_WHITELIST, READ_BLACKLIST, WRITE_BLACKLIST;

    public static String typesToString() {
      StringBuilder result = new StringBuilder();
      String prefix = "";
      for (AccessType type : AccessType.values()) {
        result.append(prefix);
        result.append(type.name());
        prefix = ", ";
      }
      return result.toString();
    }
  };

  // make it a singleton class
  public static AclAccess getInstance() {
    return AclAccessHolder.INSTANCE;
  }

  private static class AclAccessHolder {

    private static final AclAccess INSTANCE = new AclAccess();
  }

  private static String makeACLKey(AccessType access, String key) {
    return Defs.INTERNAL_PREFIX + access.name() + "_" + key;
  }

  private Set<String> lookupHelper(AccessType access, GuidInfo userInfo, String key) {
    Intercessor client = Intercessor.getInstance();
    QueryResultValue result = client.sendQuery(userInfo.getGuid(), makeACLKey(access, key));
    if (result != null) {
      return new HashSet<String>(result);
    } else {
      return null;
    }
  }

  public Set<String> lookup(AccessType access, GuidInfo userInfo, String key) {
    Set<String> result = lookupHelper(access, userInfo, key);
    if (result != null) {
      return result;
    } else {
      return new HashSet<String>();
    }
  }

  public void add(AccessType access, GuidInfo userInfo, String key, String value) {
    Intercessor client = Intercessor.getInstance();
    String aclKey = makeACLKey(access, key);
    client.sendUpdateRecordWithConfirmation(userInfo.getGuid(), aclKey, value, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public void remove(AccessType access, GuidInfo userInfo, String key, String value) {
    Intercessor client = Intercessor.getInstance();
    String aclKey = makeACLKey(access, key);
    client.sendUpdateRecordWithConfirmation(userInfo.getGuid(), aclKey, value, null, UpdateOperation.REMOVE);
  }
}
