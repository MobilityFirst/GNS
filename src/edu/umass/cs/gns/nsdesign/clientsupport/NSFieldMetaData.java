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

/*** DO NOT not use any class in package edu.umass.cs.gns.nsdesign ***/

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
  public static Set<String> lookup(MetaDataTypeName type, GuidInfo guidInfo, String key, GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException {
    return lookup(type, guidInfo.getGuid(), key, activeReplica);
  }

  /**
   * Grabs the metadata indexed by type from the field from the guid.
   * 
   * @param type
   * @param guid
   * @param key
   * @return 
   */
  public static Set<String> lookup(MetaDataTypeName type, String guid, String key, GnsReconfigurable activeReplica) throws RecordNotFoundException, FieldNotFoundException {
    String metaDateFieldName = FieldMetaData.makeFieldMetaDataKey(type, key);
    NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, metaDateFieldName);
    GNS.getLogger().fine("LOOKUP:" + nameRecord.toString());
    ResultValue result = nameRecord.getKey(metaDateFieldName);
    return new HashSet(result);

  }
}
