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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class LookupRandomGuids extends GnsCommand {

  /**
   * Creates a LookupAccountRecord instance.
   *
   * @param module
   */
  public LookupRandomGuids(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, GUIDCNT};
  }

  @Override
  public String getCommandName() {
    return LOOKUP_ACCOUNT_RECORD;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String guid = json.getString(GUID);
    int count = json.getInt(GUIDCNT);
    AccountInfo acccountInfo;
    if ((acccountInfo = AccountAccess.lookupAccountInfoFromGuid(guid, handler)) == null) {
      return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_ACCOUNT + " " + guid);
    }
    if (acccountInfo != null) {
      List<String> guids = acccountInfo.getGuids();
      if (count >= guids.size()) {
        return new CommandResponse<>(new JSONArray(guids).toString());
      } else {
        Random rand = new Random();
        List<String> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
          result.add(guids.get(rand.nextInt(guids.size())));
        }
        return new CommandResponse<>(new JSONArray(result).toString());
      }
    } else {
      return new CommandResponse<>(BAD_RESPONSE + " " + BAD_GUID + " " + guid);
    }
    // }
  }

  @Override
  public String getCommandDescription() {
    return "Returns the account info associated with the given GUID. "
            + "Returns " + BAD_GUID + " if the GUID has not been registered.";

  }
}
