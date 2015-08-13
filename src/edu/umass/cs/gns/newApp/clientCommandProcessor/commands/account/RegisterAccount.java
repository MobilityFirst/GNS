/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.AccessSupport;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.ClientUtils;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.Base64;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class RegisterAccount extends GnsCommand {

  public RegisterAccount(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, PUBLICKEY, PASSWORD};
  }

  @Override
  public String getCommandName() {
    return REGISTERACCOUNT;
  }

  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException, UnsupportedEncodingException {
    String name = json.getString(NAME);
    String publicKey = json.getString(PUBLICKEY);
    String password = json.getString(PASSWORD);
    String signature = json.optString(SIGNATURE, null);
    String message = json.optString(SIGNATUREFULLMESSAGE, null);
    byte[] publicKeyBytes = Base64.decode(publicKey);
    String guid = ClientUtils.createGuidStringFromPublicKey(publicKeyBytes);

    if (signature != null && message != null) { //FIXME: this is for temporary backward compatability... remove it. 
      if (!AccessSupport.verifySignature(publicKey, signature, message)) {
        return new CommandResponse<String>(BADRESPONSE + " " + BADSIGNATURE);
//      } else {
//        GNS.getLogger().info("########SIGNATURE VERIFIED FOR CREATE " + name);
      }
    }
    CommandResponse<String> result = AccountAccess.addAccountWithVerification(module.getHTTPHost(), name, guid, publicKey,
            password, handler);
    if (result.getReturnValue().equals(OKRESPONSE)) {
      // set up the default read access
      //FieldMetaData.add(MetaDataTypeName.READ_WHITELIST, guid, ALLFIELDS, EVERYONE, handler);
      return new CommandResponse<String>(guid);
    } else {
      return result;
    }
  }

  @Override
  public String getCommandDescription() {
    return "Creates an account GUID associated with the human readable name and the supplied public key. "
            + "Must be sign dwith the public key. "
            + "Returns a guid.";

  }
}
