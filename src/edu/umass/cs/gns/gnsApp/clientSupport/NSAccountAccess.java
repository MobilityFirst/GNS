/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.clientSupport;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gns.utils.ValuesMap;
import java.net.InetSocketAddress;
import org.json.JSONException;

import java.text.ParseException;
import org.json.JSONObject;

/**
 * Provides Name Server side support for reading and writing guid account information from the database.
 *
 * @author westy
 */
public class NSAccountAccess {

  /**
   * Obtains the guid info record from the database for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @param activeReplica
   * @return an {@link edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GuidInfo} instance
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public static GuidInfo lookupGuidInfo(String guid, GnsApplicationInterface<String> activeReplica) throws FailedDBOperationException {
    return NSAccountAccess.lookupGuidInfo(guid, false, activeReplica, null);
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * If allowQueryToOtherNSs is true and the record is not available locally
   * a query will be sent another name server to find the record.
   *
   * @param guid
   * @param allowQueryToOtherNSs
   * @param activeReplica
   * @param lnsAddress
   * @return a {@link GuidInfo} instance or null
   * @throws edu.umass.cs.gns.exceptions.FailedDBOperationException
   */
  public static GuidInfo lookupGuidInfo(String guid, boolean allowQueryToOtherNSs, 
          GnsApplicationInterface<String> activeReplica,
          InetSocketAddress lnsAddress) throws FailedDBOperationException {
    ValuesMap valuesMap = NSFieldAccess.lookupFieldAnywhere(guid, AccountAccess.GUID_INFO, activeReplica, lnsAddress);
//    ResultValue guidResult = NSFieldAccess.lookupListFieldAnywhere(guid, AccountAccess.GUID_INFO, allowQueryToOtherNSs, activeReplica,
//            lnsAddress);
    //GNS.getLogger().info("VALUESMAP=" + valuesMap.toString());
    if (valuesMap.has(AccountAccess.GUID_INFO)) {
    //if (!guidResult.isEmpty()) {
      try {
        //Object object = valuesMap.get(AccountAccess.GUID_INFO);
        //GNS.getLogger().info("CLASS=" + object.getClass().getName());
        // this is a hack
        return new GuidInfo(new JSONObject(valuesMap.get(AccountAccess.GUID_INFO).toString()));
        //return new GuidInfo(valuesMap.getJSONObject(AccountAccess.GUID_INFO));
        //return new GuidInfo(guidResult.toResultValueString());
      } catch (JSONException | ParseException e) {
        GNS.getLogger().severe("Problem parsing guidinfo: " + e);
      }
    }
    return null;
  }
}
