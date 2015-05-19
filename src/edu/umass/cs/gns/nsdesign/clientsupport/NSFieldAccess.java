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
import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
import edu.umass.cs.gns.newApp.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;
import java.net.InetSocketAddress;

/**
 * Methods for reading field information in guids on NameServers.
 *
 * Similar code for getting values from the database exists in GnsReconLookup.
 *
 * @author westy
 */
public class NSFieldAccess {

  /**
   * Looks up the value of an old-style list field in the guid on this NameServer.
   * Returns the value of a field in a GUID as a ResultValue or an empty ResultValue if field cannot be found.
   *
   * @param guid
   * @param field
   * @param activeReplica
   * @return ResultValue
   */
  public static ResultValue lookupListFieldOnThisServer(String guid, String field, GnsApplicationInterface activeReplica) throws FailedDBOperationException {
    ResultValue result = null;
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, ColumnFieldType.LIST_STRING, field);
      if (Config.debuggingEnabled) {
        GNS.getLogger().fine("LOOKUPFIELDONTHISSERVER: " + guid + " : " + field + "->" + nameRecord);
      }
      result = nameRecord.getKeyAsArray(field);
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
   * Looks up the value of a field in the guid on this NameServer.
   * 
   * Returns the value of a field in a GUID as a ValuesMap.
   *
   * @param guid
   * @param field
   * @param activeReplica
   * @return ResultValue
   */
  public static ValuesMap lookupFieldOnThisServer(String guid, String field, GnsApplicationInterface activeReplica)
          throws FailedDBOperationException {
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, ColumnFieldType.USER_JSON, field);
      if (Config.debuggingEnabled) {
        GNS.getLogger().fine("LOOKUPFIELDONTHISSERVER: " + guid + " : " + field + "->" + nameRecord);
      }
      return nameRecord.getValuesMap();
    } catch (FieldNotFoundException e) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("Field not found " + guid + " : " + field);
      }
    } catch (RecordNotFoundException e) {
      if (Config.debuggingEnabled) {
        GNS.getLogger().info("Record not found " + guid + " : " + field);
      }
    }
    return null;
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
  public static String lookupSingletonFieldOnThisServer(String recordName, String key, GnsApplicationInterface activeReplica) throws FailedDBOperationException {
    ResultValue guidResult = lookupListFieldOnThisServer(recordName, key, activeReplica);
    if (guidResult != null && !guidResult.isEmpty()) {
      return (String) guidResult.get(0);
    } else {
      return null;
    }
  }

  /**
   * Looks up the value of an old-style list field in the guid.
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
  public static ResultValue lookupListFieldAnywhere(String guid, String field, boolean allowQueryToOtherNSs,
          GnsApplicationInterface activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
    ResultValue result = lookupListFieldOnThisServer(guid, field, activeReplica);
    // if values wasn't found and the guid doesn't exist on this server and we're allowed then send a query to the LNS
    if (result.isEmpty() && !activeReplica.getDB().containsName(guid) && allowQueryToOtherNSs) {
      result = lookupListFieldQueryLNS(guid, field, activeReplica, lnsAddress);
      if (!result.isEmpty()) {
        GNS.getLogger().info("@@@@@@ Field " + field + " in " + guid + " not found on this server but was found thru LNS query.");
      }
    }
    return result;
  }

  private static ResultValue lookupListFieldQueryLNS(String guid, String field, GnsApplicationInterface activeReplica,
          InetSocketAddress lnsAddress) {
    QueryResult queryResult = LNSQueryHandler.sendListFieldQuery(guid, field, activeReplica, lnsAddress);
    if (!queryResult.isError()) {
      return queryResult.getArray(field);
    } else {
      return new ResultValue();
    }
  }

  /**
   * Looks up the value of a field in the guid.
   * If allowQueryToOtherNSs is true and guid doesn't exists on this Name Server,
   * sends a DNS query from this Name Server to a Local Name Server.
   * Returns the value of a field in a GUID as a ResultValue.
   *
   * @param guid
   * @param field
   * @param activeReplica
   * @return ValuesMap containing the value of the field or null if field cannot be found
   */
  public static ValuesMap lookupFieldLocalAndRemote(String guid, String field, GnsApplicationInterface activeReplica, 
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    ValuesMap result = lookupFieldOnThisServer(guid, field, activeReplica);
    // if values wasn't found and the guid doesn't exist on this server and we're allowed then send a query to the LNS
    if (result == null && !activeReplica.getDB().containsName(guid)) {
      result = lookupFieldQueryLNS(guid, field, activeReplica, lnsAddress);
      if (result != null) {
        GNS.getLogger().info("@@@@@@ Field " + field + " in " + guid + " not found on this server but was found thru LNS query.");
      }
    }
    return result;
  }

  private static ValuesMap lookupFieldQueryLNS(String guid, String field, GnsApplicationInterface activeReplica,
          InetSocketAddress lnsAddress) {
    QueryResult queryResult = LNSQueryHandler.sendQuery(guid, field, activeReplica, lnsAddress);
    if (!queryResult.isError()) {
      return queryResult.getValuesMap();
    } else {
      return null;
    }
  }

}
