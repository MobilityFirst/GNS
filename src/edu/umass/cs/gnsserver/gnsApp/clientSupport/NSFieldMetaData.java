/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gnsserver.gnsApp.clientSupport;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.utils.ResultValue;
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
   * @return a set of objects
   * @throws edu.umass.cs.gnsserver.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FieldNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static Set<Object> lookupOnThisNameServer(MetaDataTypeName type, GuidInfo guidInfo, String key,
          GnsApplicationInterface<String> activeReplica) throws RecordNotFoundException, FieldNotFoundException, FailedDBOperationException {
    return lookupOnThisNameServer(type, guidInfo.getGuid(), key, activeReplica);
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   *
   * @param type
   * @param guid
   * @param key
   * @param activeReplica
   * @return a set of objects
   * @throws edu.umass.cs.gnsserver.exceptions.RecordNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FieldNotFoundException
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static Set<Object> lookupOnThisNameServer(MetaDataTypeName type, String guid, String key,
          GnsApplicationInterface<String> activeReplica) throws RecordNotFoundException, FieldNotFoundException, FailedDBOperationException {
    ResultValue result = NSFieldAccess.lookupListFieldOnThisServer(guid, FieldMetaData.makeFieldMetaDataKey(type, key), activeReplica);
    if (result != null) {
      return new HashSet<Object>(result);
    } else {
      return new HashSet<Object>();
    }
  }

  /**
   * Add a value to a metadata field.
   * 
   * @param type
   * @param guid
   * @param key
   * @param value
   * @param activeReplica
   * @param lnsAddress
   */
  public static void add(MetaDataTypeName type, String guid, String key, String value, 
          GnsApplicationInterface<String> activeReplica, InetSocketAddress lnsAddress) {

    LNSUpdateHandler.sendUpdate(guid, FieldMetaData.makeFieldMetaDataKey(type, key), new ResultValue(Arrays.asList(value)), 
            UpdateOperation.SINGLE_FIELD_APPEND_OR_CREATE, activeReplica, lnsAddress);
  }

  /**
   * Remove a value from a metadata field.
   * 
   * @param type
   * @param guid
   * @param key
   * @param value
   * @param activeReplica
   * @param lnsAddress
   */
  public static void remove(MetaDataTypeName type, String guid, String key, String value, 
          GnsApplicationInterface<String> activeReplica, InetSocketAddress lnsAddress) {

    LNSUpdateHandler.sendUpdate(guid, FieldMetaData.makeFieldMetaDataKey(type, key), new ResultValue(Arrays.asList(value)), 
            UpdateOperation.SINGLE_FIELD_REMOVE, activeReplica, lnsAddress);
  }
}
