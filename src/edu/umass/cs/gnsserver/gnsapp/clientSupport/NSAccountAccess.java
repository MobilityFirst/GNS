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
package edu.umass.cs.gnsserver.gnsapp.clientSupport;

import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.GNSApplicationInterface;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.utils.ValuesMap;

import org.json.JSONException;

import java.text.ParseException;
import java.util.logging.Level;

import org.json.JSONObject;

/**
 * Provides Name Server side support for reading and writing guid account information from the database.
 *
 * @author westy
 */
public class NSAccountAccess {

  /**
   * Obtains the guid info record from the local database for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @param gnsApp
   * @return an {@link edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo} instance
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static GuidInfo lookupGuidInfoLocally(String guid,  GNSApplicationInterface<String> gnsApp) 
          throws FailedDBOperationException {
   ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldLocally(null, guid, 
            AccountAccess.GUID_INFO, gnsApp);
    if (valuesMap.has(AccountAccess.GUID_INFO)) {
      try {
        // Something simpler here?
        return new GuidInfo(new JSONObject(valuesMap.get(AccountAccess.GUID_INFO).toString()));
      } catch (JSONException | ParseException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Problem parsing guidinfo: {0}", e);
      }
    }
    return null;
  }
  
  /**
   * Obtains the guid info record from the local database or remotely if necessary for GUID given.
   * <p>
   * GUID = Globally Unique Identifier<br>
   *
   * @param guid
   * @param gnsApp
   * @return an {@link edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo} instance
   * @throws edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException
   */
  public static GuidInfo lookupGuidInfoAnywhere(InternalRequestHeader header, String guid, GNSApplicationInterface<String> gnsApp) 
          throws FailedDBOperationException {
    ValuesMap valuesMap = NSFieldAccess.lookupJSONFieldAnywhere(header, guid, 
            AccountAccess.GUID_INFO, gnsApp);
    if (valuesMap.has(AccountAccess.GUID_INFO)) {
      try {
        // Something simpler here?
        return new GuidInfo(new JSONObject(valuesMap.get(AccountAccess.GUID_INFO).toString()));
      } catch (JSONException | ParseException e) {
        GNSConfig.getLogger().log(Level.SEVERE, "Problem parsing guidinfo: {0}", e);
      }
    }
    return null;
  }
}
