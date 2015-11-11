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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Iterator;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * Note: Currently doesn't handle subGuids that are tagged! Only deletes account GUIDs that are tagged.
 *
 * @author westy
 */
public class ClearTagged extends GnsCommand {

  /**
   *
   * @param module
   */
  public ClearTagged(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME};
  }

  @Override
  public String getCommandName() {
    return CLEAR_TAGGED;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String tagName = json.getString(NAME);
    for (Iterator<?> it = handler.getAdmintercessor().collectTaggedGuids(tagName, handler).iterator(); it.hasNext();) {
      String guid = (String) it.next();
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(guid, handler);
      if (accountInfo != null) {
        AccountAccess.removeAccount(accountInfo, handler);
      }
    }
    return new CommandResponse<String>(OK_RESPONSE);
  }

  @Override
  public String getCommandDescription() {
    return "Removes all guids that contain the tag.";
  }
}
