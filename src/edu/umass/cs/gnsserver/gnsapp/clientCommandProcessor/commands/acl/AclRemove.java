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
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
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

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class AclRemove extends BasicCommand {

  /**
   *
   * @param module
   */
  public AclRemove(CommandModule module) {
    super(module);
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.AclRemove;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, ACCESSER, WRITER, ACL_TYPE, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

//  @Override
//  public String getCommandName() {
//    return ACL_REMOVE;
//  }
  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    // The guid that is losing access to this field
    String accesser = json.getString(ACCESSER);
    // allows someone other than guid to change the acl, defaults to guid
    String writer = json.optString(WRITER, guid);
    String accessType = json.getString(ACL_TYPE);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    Date timestamp = json.has(TIMESTAMP) ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client
    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return new CommandResponse<String>(BAD_RESPONSE
              + " " + BAD_ACL_TYPE + "Should be one of " + MetaDataTypeName.values().toString(),
              GNSResponseCode.BAD_ACL_TYPE_ERROR);
    }
    GNSResponseCode responseCode;
    // We need the public key

    String accessorPublicKey;
    if (EVERYONE.equals(accesser)) {
      accessorPublicKey = EVERYONE;
    } else {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = AccountAccess.lookupGuidInfo(accesser, handler, true)) == null) {
        return new CommandResponse<String>(BAD_RESPONSE + " " + BAD_GUID + " " + accesser,
                GNSResponseCode.BAD_GUID_ERROR);
      } else {
        accessorPublicKey = accessorGuidInfo.getPublicKey();
      }
    }
    if (!(responseCode = FieldMetaData.remove(access,
            guid, field, accessorPublicKey,
            writer, signature, message, timestamp, handler)).isExceptionOrError()) {
      return new CommandResponse<String>(OK_RESPONSE, GNSResponseCode.NO_ERROR);
    } else {
      return new CommandResponse<String>(responseCode.getProtocolCode(), responseCode);
    }
  }

  @Override
  public String getCommandDescription() {
    return "Updates the access control list of the given GUID's field to remove the accesser guid."
            + "Accessor should be the guid or group guid to be removed.";

  }
}
