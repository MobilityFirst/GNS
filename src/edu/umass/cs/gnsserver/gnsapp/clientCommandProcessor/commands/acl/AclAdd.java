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

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ACCESSER;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.ACL_TYPE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_ACL_TYPE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.EVERYONE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.FIELD;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.GUID;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.TIMESTAMP;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.WRITER;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class AclAdd extends AbstractCommand {

  /**
   *
   * @param module
   */
  public AclAdd(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.AclAdd;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GUID);
    String field = json.getString(FIELD);
    // The guid that wants to access this field
    String accesser = json.getString(ACCESSER);
    // allows someone other than guid to change the acl, defaults to guid
    String writer = json.optString(WRITER, guid);
    String accessType = json.getString(ACL_TYPE);
    String signature = json.getString(SIGNATURE);
    String message = json.getString(SIGNATUREFULLMESSAGE);
    Date timestamp = json.has(TIMESTAMP) ? Format.parseDateISO8601UTC(json.getString(TIMESTAMP)) : null; // can be null on older client

    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return new CommandResponse(ResponseCode.BAD_ACL_TYPE_ERROR, BAD_RESPONSE
              + " " + BAD_ACL_TYPE + "Should be one of " + Arrays.toString(MetaDataTypeName.values()));
    }
    // Lookup the public key of the guid that we're giving access to the field.
    String accessorPublicKey;
    if (EVERYONE.equals(accesser)) {
      accessorPublicKey = EVERYONE;
    } else {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = AccountAccess.lookupGuidInfoAnywhere(accesser, handler)) == null) {
        return new CommandResponse(ResponseCode.BAD_GUID_ERROR, BAD_RESPONSE + " " + BAD_GUID + " " + accesser);
      } else {
        accessorPublicKey = accessorGuidInfo.getPublicKey();
      }
    }
    // This is where we update the ACL. Put the public key of the accessing guid in the appropriate ACL list.
    ResponseCode responseCode;
    if (!(responseCode = FieldMetaData.add(access, guid, field,
            accessorPublicKey, writer, signature, message, timestamp, handler)).isExceptionOrError()) {
      return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
    } else {
      return new CommandResponse(responseCode, responseCode.getProtocolCode());
    }
  }

}
