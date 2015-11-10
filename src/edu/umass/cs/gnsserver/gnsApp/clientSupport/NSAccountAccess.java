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

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;
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
   * @return an {@link edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo} instance
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
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
   * @throws edu.umass.cs.gnsserver.exceptions.FailedDBOperationException
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
