/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientSupport;

import edu.umass.cs.gnsserver.gnsApp.QueryResult;
import edu.umass.cs.gnsserver.database.ColumnFieldType;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.exceptions.FieldNotFoundException;
import edu.umass.cs.gnsserver.exceptions.RecordNotFoundException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.ValuesMap;
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
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static ResultValue lookupListFieldOnThisServer(String guid, String field, 
          GnsApplicationInterface<String> activeReplica) throws FailedDBOperationException {
    ResultValue result = null;
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, ColumnFieldType.LIST_STRING, field);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().fine("LOOKUPFIELDONTHISSERVER: " + guid + " : " + field + "->" + nameRecord);
      }
      result = nameRecord.getKeyAsArray(field);
    } catch (FieldNotFoundException e) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("Field not found " + guid + " : " + field);
      }
    } catch (RecordNotFoundException e) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
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
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static ValuesMap lookupFieldOnThisServer(String guid, String field, 
          GnsApplicationInterface<String> activeReplica)
          throws FailedDBOperationException {
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, ColumnFieldType.USER_JSON, field);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().fine("LOOKUPFIELDONTHISSERVER: " + guid + " : " + field + "->" + nameRecord);
      }
      return nameRecord.getValuesMap();
    } catch (FieldNotFoundException e) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("Field not found " + guid + " : " + field);
      }
    } catch (RecordNotFoundException e) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
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
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static String lookupSingletonFieldOnThisServer(String recordName, String key, 
          GnsApplicationInterface<String> activeReplica) throws FailedDBOperationException {
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
   * @param lnsAddress
   * @return ResultValue containing the value of the field or an empty ResultValue if field cannot be found
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static ResultValue lookupListFieldAnywhere(String guid, String field, boolean allowQueryToOtherNSs,
          GnsApplicationInterface<String> activeReplica, InetSocketAddress lnsAddress) throws FailedDBOperationException {
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

  private static ResultValue lookupListFieldQueryLNS(String guid, String field, GnsApplicationInterface<String> activeReplica,
          InetSocketAddress lnsAddress) {
    QueryResult<String> queryResult = LNSQueryHandler.sendListFieldQuery(guid, field, activeReplica, lnsAddress);
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
   * @param lnsAddress
   * @return ValuesMap containing the value of the field or null if field cannot be found
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
   */
  public static ValuesMap lookupFieldAnywhere(String guid, String field, GnsApplicationInterface<String> activeReplica, 
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

  private static ValuesMap lookupFieldQueryLNS(String guid, String field, GnsApplicationInterface<String> activeReplica,
          InetSocketAddress lnsAddress) {
    QueryResult<String> queryResult= LNSQueryHandler.sendQuery(guid, field, activeReplica, lnsAddress);
    if (!queryResult.isError()) {
      return queryResult.getValuesMap();
    } else {
      return null;
    }
  }

}
