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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.UpdateOperation;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;
import edu.umass.cs.gnsserver.utils.ResultValue;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class SetFieldNull extends AbstractUpdate {

  /**
   *
   * @param module
   */
  public SetFieldNull(CommandModule module) {
    super(module);
  }

  /**
   * Return the update operation.
   * 
   * @return an {@link UpdateOperation}
   */
  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_SET_FIELD_NULL;
  }

  @Override
  public String getCommandName() {
    return SET_FIELD_NULL;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Sets the field to contain a null value."
            + " Field must be writeable by the WRITER guid.";
  }
}
