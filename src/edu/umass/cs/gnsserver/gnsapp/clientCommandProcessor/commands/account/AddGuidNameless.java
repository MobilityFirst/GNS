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
import edu.umass.cs.gnscommon.SharedGuidUtils;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSAccessSupport;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;

/**
 * Command to add a guid.
 *
 * @author westy
 */
public class AddGuidNameless extends AbstractCommand {

	/**
	 * Creates an AddGuid instance.
	 *
	 * @param module
	 */
	public AddGuidNameless(CommandModule module) {
		super(module);
	}

	/**
	 *
	 * @return the command type
	 */
	@Override
	public CommandType getCommandType() {
		return CommandType.AddGuidNameless;
	}

	@Override
	public CommandResponse execute(InternalRequestHeader header, CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
		JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
		JSONObject json = commandPacket.getCommand();
		String name = json.optString(GNSProtocol.NAME.toString(), null);
		String accountGuid = json.getString(GNSProtocol.GUID.toString());
		String publicKey = json.optString(GNSProtocol.PUBLIC_KEY.toString(), null);
		String signature = json.getString(GNSProtocol.SIGNATURE.toString());
		String message = json.getString(GNSProtocol.SIGNATUREFULLMESSAGE.toString());

		Set<InetSocketAddress> activesSet = json.has(GNSProtocol.ACTIVES_SET.toString())
			? Util.getSocketAddresses(json.getJSONArray(GNSProtocol.ACTIVES_SET.toString())): null;

		if(publicKey==null)
			return new CommandResponse(ResponseCode.JSON_PARSE_ERROR,
				GNSProtocol.BAD_RESPONSE.toString() + " Missing required " +
					"public key argument" );

		String 	newGuid = SharedGuidUtils.createGuidStringFromPublicKey(publicKey.getBytes());

		GuidInfo accountGuidInfo;
		if ((accountGuidInfo = AccountAccess.lookupGuidInfoAnywhere(header, accountGuid, handler)) == null) {
			return new CommandResponse(ResponseCode.BAD_GUID_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_GUID.toString() + " " + accountGuid);
		}
		if (NSAccessSupport.verifySignature(accountGuidInfo.getPublicKey(), signature, message)) {
			AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuidAnywhere(header, accountGuid, handler);
			if (accountInfo == null) {
				return new CommandResponse(ResponseCode.BAD_ACCOUNT_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_ACCOUNT.toString() + " " + accountGuid);
			}
			if (!accountInfo.isVerified()) {
				return new CommandResponse(ResponseCode.VERIFICATION_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.VERIFICATION_ERROR.toString() + " Account not verified");
			} else if (accountInfo.getGuids().size() > Config.getGlobalInt(GNSConfig.GNSC.ACCOUNT_GUID_MAX_SUBGUIDS)) {
				return new CommandResponse(ResponseCode.TOO_MANY_GUIDS_EXCEPTION, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.TOO_MANY_GUIDS.toString());
			} else {
				CommandResponse result = AccountAccess.addGuid(header, commandPacket,
					accountInfo, accountGuidInfo, name, newGuid, publicKey, handler, activesSet);
				if (result.getExceptionOrErrorCode().isOKResult()) {
					// Everything is hunkey dorey so return the new guid
					return new CommandResponse(ResponseCode.NO_ERROR, newGuid);
				} else {
					// Otherwise return the error response
					return result;
				}
			}
		} else {
			// Signature verification failed
			return new CommandResponse(ResponseCode.SIGNATURE_ERROR, GNSProtocol.BAD_RESPONSE.toString() + " " + GNSProtocol.BAD_SIGNATURE.toString());
		}
		//}
	}

}