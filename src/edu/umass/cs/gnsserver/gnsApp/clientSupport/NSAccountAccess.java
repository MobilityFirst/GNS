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
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.gnsApp.recordmap.BasicRecordMap;
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
   * @return an {@link edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GuidInfo} instance
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static GuidInfo lookupGuidInfo(String guid,  GnsApplicationInterface<String> gnsApp) 
          throws FailedDBOperationException {
    return NSAccountAccess.lookupGuidInfo(guid, false, gnsApp);
  }

  /**
   * Obtains the guid info record from the database for GUID given.
   * If allowQueryToOtherNSs is true and the record is not available locally
   * a query will be sent another name server to find the record.
   *
   * @param guid
   * @param allowQueryToOtherNSs
   * @param gnsApp
   * @return a {@link GuidInfo} instance or null
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static GuidInfo lookupGuidInfo(String guid, boolean allowQueryToOtherNSs, 
           GnsApplicationInterface<String> gnsApp) throws FailedDBOperationException {
    ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldAnywhere(guid, 
            AccountAccess.GUID_INFO, gnsApp);
    if (valuesMap.has(AccountAccess.GUID_INFO)) {
      try {
        // Something simpler here?
        return new GuidInfo(new JSONObject(valuesMap.get(AccountAccess.GUID_INFO).toString()));
      } catch (JSONException | ParseException e) {
        GNS.getLogger().severe("Problem parsing guidinfo: " + e);
      }
    }
    return null;
  }
}
