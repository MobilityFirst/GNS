/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.util.NSResponseCode;
import java.util.HashSet;
import java.util.Set;

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class FieldMetaDataV1 {

  private static final String MetaDataPrefix = "_MD_";

  /**
   * Creates the metadata field name for the given metadata type and field name.
   * 
   * @param metaDataType
   * @param key
   * @return
   */
  public static String makeFieldMetaDataKey(MetaDataTypeName metaDataType, String key) {
    return InternalField.makeInternalFieldString(MetaDataPrefix + metaDataType.name() + "_" + key);
  }

  /**
   * Returns true if the string the name of a meta field.
   * 
   * @param string
   * @return
   */
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
   * @param reader
   * @param message
   * @param signature
   * @return 
   */
  public static Set<String> lookup(MetaDataTypeName type, String guid, String key, String reader, String signature, String message) {
    String metaDataKey = makeFieldMetaDataKey(type, key);
    QueryResult result = LocalNameServer.getIntercessor().sendQuery(guid, metaDataKey, reader, signature, message, ColumnFieldType.LIST_STRING);
    if (!result.isError()) {
      return new HashSet<String>(result.getArray(metaDataKey).toStringSet());
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
   * @param writer 
   * @param signature 
   * @param message 
   * @return  
   */
  public static NSResponseCode add(MetaDataTypeName type, String guid, String key, String value, String writer, String signature, String message) {
    return LocalNameServer.getIntercessor().sendUpdateRecord(guid, makeFieldMetaDataKey(type, key), value, null, -1, UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE,
            writer, signature, message);
  }

  // Deprecated because it doesn't require authentication.
  // Still used in account registration

  /**
   *  Adds a value to the metadata of the field in the guid.
   *  Doesn't require authentication.
   * 
   * @param type
   * @param guid
   * @param key
   * @param value
   * @deprecated
   */
    @Deprecated
  public static void add(MetaDataTypeName type, String guid, String key, String value) {

    String metaDataKey = makeFieldMetaDataKey(type, key);
    LocalNameServer.getIntercessor().sendUpdateRecordBypassingAuthentication(guid, metaDataKey, value, null, UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
  }

  /**
   * Removes the value from the metadata of the field in the guid.
   * 
   * @param type
   * @param guid
   * @param key
   * @param value
   * @param writer
   * @param signature
   * @param message
   * @return
   */
  public static NSResponseCode remove(MetaDataTypeName type, String guid, String key, String value, String writer, String signature, String message) {
    return LocalNameServer.getIntercessor().sendUpdateRecord(guid, makeFieldMetaDataKey(type, key), value, null, -1, UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message);
  }

  //

  /**
   *
   */
    public static String Version = "$Revision$";
}
