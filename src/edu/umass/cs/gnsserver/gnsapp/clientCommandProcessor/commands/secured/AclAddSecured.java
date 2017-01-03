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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.secured;

import edu.umass.cs.gnscommon.utils.Format;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;

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
public class AclAddSecured extends AbstractCommand {

  /**
   *
   * @param module
   */
  public AclAddSecured(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.AclAddSecured;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, ParseException {
    String guid = json.getString(GNSProtocol.GUID.toString());
    String field = json.getString(GNSProtocol.FIELD.toString());
    // The guid that wants to access this field
    String accesser = json.getString(GNSProtocol.ACCESSER.toString());
    String accessType = json.getString(GNSProtocol.ACL_TYPE.toString());
    Date timestamp = json.has(GNSProtocol.TIMESTAMP.toString()) ? Format.parseDateISO8601UTC(json.getString(GNSProtocol.TIMESTAMP.toString())) : null; // can be null on older client

    MetaDataTypeName access;
    if ((access = MetaDataTypeName.valueOf(accessType)) == null) {
      return new CommandResponse(ResponseCode.BAD_ACL_TYPE_ERROR, GNSProtocol.BAD_RESPONSE.toString()
              + " " + GNSProtocol.BAD_ACL_TYPE.toString() + "Should be one of " + Arrays.toString(MetaDataTypeName.values()));
    }
    // Lookup the public key of the guid that we're giving access to the field.
    String accessorPublicKey;
    if (GNSProtocol.EVERYONE.toString().equals(accesser)) {
      accessorPublicKey = GNSProtocol.EVERYONE.toString();
    } else {
      GuidInfo accessorGuidInfo;
      if ((accessorGuidInfo = AccountAccess.lookupGuidInfoAnywhere(accesser, handler)) == null) {
        return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + accesser);
      } else {
        accessorPublicKey = accessorGuidInfo.getPublicKey();
      }
    }
    // This is where we update the ACL. Put the public key of the accessing guid in the appropriate ACL list.
    ResponseCode responseCode;
    if (!(responseCode = FieldMetaData.add(header, access, guid, field,
            accessorPublicKey, 
            GNSProtocol.INTERNAL_QUERIER.toString(),
            //GNSConfig.getInternalOpSecret(),
            null, null, timestamp, handler)).isExceptionOrError()) {
      return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
    } else {
      return new CommandResponse(responseCode, responseCode.getProtocolCode());
    }
  }

}
