/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.FieldMetaData;
import edu.umass.cs.gns.clientsupport.GuidInfo;
import edu.umass.cs.gns.clientsupport.MetaDataTypeName;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;

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
  public static Set<String> lookupOnThisNameServer(MetaDataTypeName type, GuidInfo guidInfo, String key, GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException {
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
  public static Set<String> lookupOnThisNameServer(MetaDataTypeName type, String guid, String key, GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException {
    ResultValue result = NSFieldAccess.lookupFieldOnThisNameServer(guid, FieldMetaData.makeFieldMetaDataKey(type, key), activeReplica);
    if (result != null) {
      return new HashSet(result);
    } else {
      return new HashSet();
    }
  }
}
