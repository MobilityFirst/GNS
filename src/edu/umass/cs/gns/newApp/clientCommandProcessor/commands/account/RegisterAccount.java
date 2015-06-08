/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.account;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.ClientUtils;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.FieldMetaData;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.MetaDataTypeName;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gns.util.Base64;
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
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
      String name = json.getString(NAME);
      String publicKey = json.getString(PUBLICKEY);
      String password = json.getString(PASSWORD);
      byte[] publicKeyBytes = Base64.decode(publicKey);
      String guid = ClientUtils.createGuidStringFromPublicKey(publicKeyBytes);
      
      CommandResponse result = AccountAccess.addAccountWithVerification(module.getHTTPHost(), name, guid, publicKey, 
              password, handler);
      if (result.getReturnValue().equals(OKRESPONSE)) {
        // set up the default read access
        FieldMetaData.add(MetaDataTypeName.READ_WHITELIST, guid, ALLFIELDS, EVERYONE, handler);
        return new CommandResponse(guid);
      } else {
        return result;
      }
  }
  
  @Override
  public String getCommandDescription() {
    return "Creates a GUID associated with the the human readable name "
            + "(a human readable name) and the supplied publickey. Returns a guid.";

  }
}
