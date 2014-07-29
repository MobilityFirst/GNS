/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.util.NSResponseCode;
import java.util.HashSet;
import java.util.Set;

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class FieldMetaData {

  private static final String MetaDataPrefix = "_MD_";

  public static String makeFieldMetaDataKey(MetaDataTypeName metaDataType, String key) {
    return InternalField.makeInternalFieldString(MetaDataPrefix + metaDataType.name() + "_" + key);
  }

  public static boolean isMetaDataField(String string) {
    if (InternalField.isInternalField(string)) {
      return string.substring(InternalField.getPrefixLength()).startsWith(MetaDataPrefix);
    } else {
      return false;
    }
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   * 
   * @param type
   * @param guid
   * @param key
   * @return 
   */
  public static Set<String> lookup(MetaDataTypeName type, String guid, String key, String reader, String signature, String message) {
    String metaDataKey = makeFieldMetaDataKey(type, key);
    QueryResult result = Intercessor.sendQuery(guid, metaDataKey, reader, signature, message);
    if (!result.isError()) {
      return new HashSet<String>(result.get(metaDataKey).toStringSet());
    } else {
      return new HashSet<String>();
    }
  }

  /**
   * Adds a value to the metadata of the field in the guid.
   * 
   * @param type
   * @param guid
   * @param key
   * @param value 
   */
  public static NSResponseCode add(MetaDataTypeName type, String guid, String key, String value, String writer, String signature, String message) {
    return Intercessor.sendUpdateRecord(guid, makeFieldMetaDataKey(type, key), value, null, -1, UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE,
            writer, signature, message);
  }

  // Deprecated because it doesn't require use authentication
  // Still used in account registration
  @Deprecated
  public static void add(MetaDataTypeName type, String guid, String key, String value) {

    String metaDataKey = makeFieldMetaDataKey(type, key);
    Intercessor.sendUpdateRecordBypassingAuthentication(guid, metaDataKey, value, null, UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
  }

  public static NSResponseCode remove(MetaDataTypeName type, String guid, String key, String value, String writer, String signature, String message) {
    return Intercessor.sendUpdateRecord(guid, makeFieldMetaDataKey(type, key), value, null, -1, UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message);
  }

  //
  public static String Version = "$Revision$";
}
