/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;

/**
 * Methods for reading field information in guids on NameServers.
 *
 * @author westy
 */
public class NSFieldAccess {

  /**
   * Looks up the value of field in the guid on this NameServer.
   * Returns the value of a field in a GUID as a ResultValue or an empty ResultValue if field cannot be found.
   *
   * @param guid
   * @param field
   * @param activeReplica
   * @return ResultValue
   */
  public static ResultValue lookupFieldOnThisServer(String guid, String field, GnsReconfigurableInterface activeReplica) throws FailedDBOperationException {
    ResultValue result = null;
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, ColumnFieldType.LIST_STRING, field);
      if (Config.debuggingEnabled) {
        GNS.getLogger().fine("LOOKUPFIELDONTHISSERVER: " + guid + " : " + field + "->" + nameRecord);
      }
      result = nameRecord.getKey(field);
    } catch (FieldNotFoundException e) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("Field not found " + guid + " : " + field);
      }
    } catch (RecordNotFoundException e) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("Record not found " + guid + " : " + field);
      }
    }
    if (result != null) {
      return result;
    } else {
      return new ResultValue();
    }
  }

  /**
   * Looks up the first element of field in the guid on this NameServer as a String.
   * Returns null if the field or the record cannot be found.
   *
   * @param recordName
   * @param key
   * @param activeReplica
   * @return a string representing the first value in field
   */
  public static String lookupSingletonFieldOnThisServer(String recordName, String key, GnsReconfigurableInterface activeReplica) throws FailedDBOperationException {
    ResultValue guidResult = lookupFieldOnThisServer(recordName, key, activeReplica);
    if (guidResult != null && !guidResult.isEmpty()) {
      return (String) guidResult.get(0);
    } else {
      return null;
    }
  }

  private static ResultValue lookupFieldQueryLNS(String guid, String field, GnsReconfigurableInterface activeReplica) {
    QueryResult queryResult = LNSQueryHandler.sendQuery(guid, field, activeReplica);
    if (!queryResult.isError()) {
      return queryResult.getArray(field);
    } else {
      return new ResultValue();
    }
  }

  /**
   * Looks up the value of field in the guid.
   * If allowQueryToOtherNSs is true and guid doesn't exists on this Name Server,
   * sends a DNS query from this Name Server to a Local Name Server.
   * Returns the value of a field in a GUID as a ResultValue.
   *
   * @param guid
   * @param field
   * @param allowQueryToOtherNSs
   * @param activeReplica
   * @return ResultValue containing the value of the field or an empty ResultValue if field cannot be found
   */
  public static ResultValue lookupField(String guid, String field, boolean allowQueryToOtherNSs, GnsReconfigurableInterface activeReplica) throws FailedDBOperationException {
    ResultValue result = lookupFieldOnThisServer(guid, field, activeReplica);
    // if values wasn't found and the guid doesn't exist on this server and we're allowed then send a query to the LNS
    if (result.isEmpty() && !activeReplica.getDB().containsName(guid) && allowQueryToOtherNSs) {
      result = lookupFieldQueryLNS(guid, field, activeReplica);
      if (!result.isEmpty()) {
        GNS.getLogger().info("@@@@@@ Field " + field + " in " + guid + " not found on this server but was found thru LNS query.");
      }
    }
    return result;
  }

}
