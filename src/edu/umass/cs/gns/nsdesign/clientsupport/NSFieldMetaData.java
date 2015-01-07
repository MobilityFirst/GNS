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
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import java.net.InetSocketAddress;
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
   * @param activeReplica
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public static Set<Object> lookupOnThisNameServer(MetaDataTypeName type, GuidInfo guidInfo, String key,
          GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException, FailedDBOperationException {
    return lookupOnThisNameServer(type, guidInfo.getGuid(), key, activeReplica);
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param type
   * @param guid
   * @param key
   * @param activeReplica
   * @return
   * @throws edu.umass.cs.gns.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gns.exceptions.FieldNotFoundException
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public static Set<Object> lookupOnThisNameServer(MetaDataTypeName type, String guid, String key,
          GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException, FailedDBOperationException {
    ResultValue result = NSFieldAccess.lookupListFieldOnThisServer(guid, FieldMetaData.makeFieldMetaDataKey(type, key), activeReplica);
    if (result != null) {
      return new HashSet<Object>(result);
    } else {
      return new HashSet<Object>();
    }
  }

  public static void add(MetaDataTypeName type, String guid, String key, String value, 
          GnsReconfigurableInterface activeReplica, InetSocketAddress lnsAddress) {

    LNSUpdateHandler.sendUpdate(guid, FieldMetaData.makeFieldMetaDataKey(type, key), new ResultValue(Arrays.asList(value)), 
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, activeReplica, lnsAddress);
  }

  public static void remove(MetaDataTypeName type, String guid, String key, String value, 
          GnsReconfigurable activeReplica, InetSocketAddress lnsAddress) {

    LNSUpdateHandler.sendUpdate(guid, FieldMetaData.makeFieldMetaDataKey(type, key), new ResultValue(Arrays.asList(value)), 
            UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica, lnsAddress);
  }
}
