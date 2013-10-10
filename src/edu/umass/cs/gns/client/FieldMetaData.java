package edu.umass.cs.gns.client;

//import edu.umass.cs.gns.packet.QueryResultValue;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.ResultValue;
import java.util.HashSet;
import java.util.Set;

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class FieldMetaData {

  public FieldMetaData() {
  }

  public enum MetaDataTypeName {

    READ_WHITELIST, WRITE_WHITELIST, READ_BLACKLIST, WRITE_BLACKLIST, TIMESTAMP;

    public static String typesToString() {
      StringBuilder result = new StringBuilder();
      String prefix = "";
      for (MetaDataTypeName type : MetaDataTypeName.values()) {
        result.append(prefix);
        result.append(type.name());
        prefix = ", ";
      }
      return result.toString();
    }
  };

  // make it a singleton class
  public static FieldMetaData getInstance() {
    return FieldMetaDataHolder.INSTANCE;
  }

  private static class FieldMetaDataHolder {

    private static final FieldMetaData INSTANCE = new FieldMetaData();
  }

  private static String makeFieldMetaDataKey(MetaDataTypeName metaDataType, String key) {
    return GNS.makeInternalField(metaDataType.name() + "_" + key);
  }

  public Set<String> lookup(MetaDataTypeName access, GuidInfo userInfo, String key) {
    Intercessor client = Intercessor.getInstance();
    ResultValue result = client.sendQuery(userInfo.getGuid(), makeFieldMetaDataKey(access, key));
    if (result != null) {
      return new HashSet<String>(result.toStringSet());
    } else {
      return new HashSet<String>();
    }
  }

  public void add(MetaDataTypeName access, GuidInfo userInfo, String key, String value) {
    Intercessor client = Intercessor.getInstance();
    String metaDataKey = makeFieldMetaDataKey(access, key);
    client.sendUpdateRecordWithConfirmation(userInfo.getGuid(), metaDataKey, value, null, UpdateOperation.APPEND_OR_CREATE);
  }

  public void remove(MetaDataTypeName access, GuidInfo userInfo, String key, String value) {
    Intercessor client = Intercessor.getInstance();
    String metaDataKey = makeFieldMetaDataKey(access, key);
    client.sendUpdateRecordWithConfirmation(userInfo.getGuid(), metaDataKey, value, null, UpdateOperation.REMOVE);
  }
  public static String Version = "$Revision$";
}
