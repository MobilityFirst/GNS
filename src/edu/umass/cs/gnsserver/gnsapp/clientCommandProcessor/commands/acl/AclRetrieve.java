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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.acl;

import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.BasicCommand;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class AclRetrieve extends BasicCommand {

  /**
   *
   * @param module
   */
  public AclRetrieve(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.AclRetrieve;
  }

  

//  @Override
//  public String getCommandName() {
//    return ACL_RETRIEVE;
//  }
  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    String accessType = json.getString(ACL_TYPE);
    // allows someone other than guid to read acl, defaults to guid
    String reader = json.optString(READER, guid);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    Date timestamp = json.has(TIMESTAMP) ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client

    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return new CommandResponse(GNSResponseCode.BAD_ACL_TYPE_ERROR, BAD_RESPONSE + " " + BAD_ACL_TYPE
              + "Should be one of " + MetaDataTypeName.values().toString());
    }
    JSONArray guids = SharedGuidUtils.convertPublicKeysToGuids(new JSONArray(FieldMetaData.lookup(access,
            guid, field, reader, signature, message, timestamp, handler)));
    return new CommandResponse(GNSResponseCode.NO_ERROR, guids.toString());
  }

  
}
