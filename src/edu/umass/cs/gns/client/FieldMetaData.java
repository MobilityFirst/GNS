/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
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

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   * 
   * @param type
   * @param guidInfo
   * @param key
   * @return 
   */
  public Set<String> lookup(MetaDataTypeName type, GuidInfo guidInfo, String key) {
    return lookup(type, guidInfo.getGuid(), key);
  }
  
  /**
   * Grabs the metadata indexed by type from the field from the guid.
   * 
   * @param type
   * @param guid
   * @param key
   * @return 
   */
  public Set<String> lookup(MetaDataTypeName type, String guid, String key) {
    Intercessor client = Intercessor.getInstance();
    ResultValue result = client.sendQuery(guid, makeFieldMetaDataKey(type, key));
    if (result != null) {
      return new HashSet<String>(result.toStringSet());
    } else {
      return new HashSet<String>();
    }
  }

  /**
   * Adds a value to the metadata of the field in the guid.
   * 
   * @param type
   * @param userInfo
   * @param key
   * @param value 
   */
  public void add(MetaDataTypeName type, GuidInfo userInfo, String key, String value) {
    add(type, userInfo.getGuid(), key, value);
  }
  
  /**
   * Adds a value to the metadata of the field in the guid.
   * 
   * @param type
   * @param guid
   * @param key
   * @param value 
   */
   public void add(MetaDataTypeName type, String guid, String key, String value) {
    Intercessor client = Intercessor.getInstance();
    String metaDataKey = makeFieldMetaDataKey(type, key);
    client.sendUpdateRecordWithConfirmation(guid, metaDataKey, value, null, UpdateOperation.APPEND_OR_CREATE);
  }

  /**
   * Removes a value from the metadata of the field in the guid.
   * 
   * @param type
   * @param userInfo
   * @param key
   * @param value 
   */
  public void remove(MetaDataTypeName type, GuidInfo userInfo, String key, String value) {
    remove(type, userInfo.getGuid(), key, value);
  }
  
  public void remove(MetaDataTypeName type, String guid, String key, String value) {
    Intercessor client = Intercessor.getInstance();
    String metaDataKey = makeFieldMetaDataKey(type, key);
    client.sendUpdateRecordWithConfirmation(guid, metaDataKey, value, null, UpdateOperation.REMOVE);
  }
  //
  public static String Version = "$Revision$";
}
