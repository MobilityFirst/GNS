/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.FieldMetaData;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.clientsupport.UpdateOperation;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Implements metadata on fields.
 *
 * @author westy
 */
public class NSFieldMetaData {

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param type
   * @param guidInfo
   * @param key
   * @return
   */
  public static Set<String> lookupOnThisNameServer(MetaDataTypeName type, GuidInfo guidInfo, String key,
          GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException {
    return lookupOnThisNameServer(type, guidInfo.getGuid(), key, activeReplica);
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param type
   * @param guid
   * @param key
   * @return
   */
  public static Set<String> lookupOnThisNameServer(MetaDataTypeName type, String guid, String key,
          GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException {
    ResultValue result = NSFieldAccess.lookupFieldOnThisServer(guid, FieldMetaData.makeFieldMetaDataKey(type, key), activeReplica);
    if (result != null) {
      return new HashSet(result);
    } else {
      return new HashSet();
    }
  }

  public static void add(MetaDataTypeName type, String guid, String key, String value, GnsReconfigurableInterface activeReplica) {

    String metaDataKey = FieldMetaData.makeFieldMetaDataKey(type, key);
    LNSUpdateHandler.sendUpdate(guid, metaDataKey, new ResultValue(Arrays.asList(value)), UpdateOperation.APPEND_OR_CREATE, activeReplica);
  }

  public static void remove(MetaDataTypeName type, String guid, String key, String value, GnsReconfigurable activeReplica) {

    String metaDataKey = FieldMetaData.makeFieldMetaDataKey(type, key);
    LNSUpdateHandler.sendUpdate(guid, metaDataKey, new ResultValue(Arrays.asList(value)), UpdateOperation.REMOVE, activeReplica);
  }
}
