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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnscommon.CommandType;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class ReadArrayUnsigned extends ReadArray {

  /**
   *
   * @param module
   */
  public ReadArrayUnsigned(CommandModule module) {
    super(module);
  }
  
  @Override
  public CommandType getCommandType() {
    return CommandType.ReadArrayUnsigned;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD};
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    // Tells the lookup handler that we don't need to authenticate.
    // Will be moved to the client and will something more secure in the future.
    json.put(READER, MAGIC_STRING);
    return super.execute(json, handler);
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS. Does not require authentication but field must be set to be readable by everyone."
            + " Values are always returned as a JSON list."
            + " Specify " + ALL_FIELDS + " as the <field> to return all fields. ";
  }
}
