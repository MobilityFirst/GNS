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
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.account;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;

/**
 *
 * @author westy
 */
public class RemoveAccount extends AbstractCommand {

  /**
   * Creates a RemoveAccount instance.
   *
   * @param module
   */
  public RemoveAccount(CommandModule module) {
    super(module);
  }

  /**
   *
   * @return the command type
   */
  @Override
  public CommandType getCommandType() {
    return CommandType.RemoveAccount;
  }

  @Override
  public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException,
          InternalRequestException {
    JSONObject json = commandPacket.getCommand();
    // The name of the account we are removing.
    String name = json.getString(GNSProtocol.NAME.toString());
    // The guid of the account we are removing.
    String guid = json.getString(GNSProtocol.GUID.toString());
    String signature = json.getString(GNSProtocol.SIGNATURE.toString());
    String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());
    GuidInfo guidInfo;
    // We don't use cache in this lookup because cache on a deletion is not updated. A GUID
    // might be deleted in the database but still be present in the cache.
    if( (guidInfo = AccountAccess.lookupGuidInfo(header, guid, handler, true, false)) == null) {
      // Removing a non-existant guid is not longer an error.
      return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString());
    }
    if (NSAccessSupport.verifySignature(guidInfo.getPublicKey(), signature, message)) {
    	// We should always lookup an account info using its GUID and not the HRN.
    	// In the account removal protocol, we delete a HRN before deleting 
    	// the account guid record, so it may be the case that the HRN is removed
    	// but the account guid is not removed, which will cause a retry of the 
    	// account GUID remove operation. So, for a successful retry and for
    	// the operation to be idempotent, we should lookup account info using the GUID.
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(header, guid, handler);
      if (accountInfo != null) {
        return AccountAccess.removeAccount(header, commandPacket, accountInfo, handler);
      } else {
    	  // aditya: Changing from ResponseCode.BAD_ACCOUNT_ERROR to NO_ERROR. As, In RemoveAccount, we want to
    	  // remove the account, but there is no accountInfo anywhere in the GNS, so it has already been
    	  // deleted by a previous or concurrent remove.
        return new CommandResponse(ResponseCode.NO_ERROR, GNSProtocol.OK_RESPONSE.toString() 
        		+ " Couldn't find the account information for "+name+" because account is already removed." );
      }
    } else {
      return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
    }
  }

}
