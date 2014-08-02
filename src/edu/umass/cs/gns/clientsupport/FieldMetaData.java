/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.util.NSResponseCode;
import java.util.HashSet;
import java.util.Set;

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class FieldMetaData {
  
  /**
   * Creates a key for looking up metadata in a guid record.
   * 
   * @param metaDataType
   * @param key
   * @return 
   */
  public static String makeFieldMetaDataKey(MetaDataTypeName metaDataType, String key) {
    return metaDataType.getFieldPath() + "." + key;
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
    return Intercessor.sendUpdateRecord(guid, makeFieldMetaDataKey(type, key), value, null, -1, UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE,
            writer, signature, message);
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   * 
   * @param type
   * @param guid
   * @param key
   * @param reader * @param key
   * @param signature
   * @param message
   * @return 
   */
  public static Set<String> lookup(MetaDataTypeName type, String guid, String key, String reader, String signature, String message) {
    QueryResult result = Intercessor.sendQuery(guid, makeFieldMetaDataKey(type, key), reader, signature, message, ColumnFieldType.LIST_STRING);
    if (!result.isError()) {
      return new HashSet<String>(result.getArray(makeFieldMetaDataKey(type, key)).toStringSet());
    } else {
      return new HashSet<String>();
    }
  }

  public static void add(MetaDataTypeName type, String guid, String key, String value) {
    Intercessor.sendUpdateRecordBypassingAuthentication(guid, makeFieldMetaDataKey(type, key), value, null, UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE);
  }

  public static NSResponseCode remove(MetaDataTypeName type, String guid, String key, String value, String writer, String signature, String message) {
    return Intercessor.sendUpdateRecord(guid, makeFieldMetaDataKey(type, key), value, null, -1, UpdateOperation.SINGLE_FIELD_REMOVE, writer, signature, message);
  }

  //
  public static String Version = "$Revision$";
}
