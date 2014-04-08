/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.util.ResultValue;

/**
 * Methods for reading field information in guids from this NameServer.
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
  public static ResultValue lookupFieldOnThisNameServer(String guid, String field, GnsReconfigurable activeReplica) {
    ResultValue result = null;
    try {
      NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getDB(), guid, null, field);
      result = nameRecord.getKey(field);
    } catch (FieldNotFoundException e) {
    } catch (RecordNotFoundException e) {
    }
    if (result != null) {
      return result;
    } else {
      return new ResultValue();
    }
  }

  private static ResultValue lookupFieldQueryLNS(String guid, String field, GnsReconfigurable activeReplica) {
    QueryResult queryResult = LNSQueryHandler.sendQuery(guid, field, activeReplica);
    if (!queryResult.isError()) {
      return queryResult.get(field);
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
  public static ResultValue lookupField(String guid, String field, boolean allowQueryToOtherNSs, GnsReconfigurable activeReplica) {
    ResultValue result = lookupFieldOnThisNameServer(guid, field, activeReplica);
    // and if we're allowed, send a query to the LNS
    if (result.isEmpty() && allowQueryToOtherNSs) {
      result = lookupFieldQueryLNS(guid, field, activeReplica);
    }
    return result;
  }

}
