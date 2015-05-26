/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.account;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.AccountAccess;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.AccountInfo;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GuidInfo;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
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
public class RemoveAlias extends GnsCommand {
  
  public RemoveAlias(CommandModule module) {
    super(module);
  }
  
  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, NAME, SIGNATURE, SIGNATUREFULLMESSAGE};
  }
  
  @Override
  public String getCommandName() {
    return REMOVEALIAS;
  }
  
  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
//    if (CommandDefs.handleAcccountCommandsAtNameServer) {
//      return LNSToNSCommandRequestHandler.sendCommandRequest(json);
//    } else {
      String guid = json.getString(GUID);
      String name = json.getString(NAME);
      String signature = json.getString(SIGNATURE);
      String message = json.getString(SIGNATUREFULLMESSAGE);
      GuidInfo guidInfo;
      if ((guidInfo = AccountAccess.lookupGuidInfo(guid, handler)) == null) {
        return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + guid);
      }
      AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(guid, handler);
      if (accountInfo == null) {
        return new CommandResponse(BADRESPONSE + " " + BADACCOUNT + " " + guid);
      }
      return AccountAccess.removeAlias(accountInfo, name, guid, signature, message, handler);
      
//      GuidInfo guidInfo;
//      if ((guidInfo = AccountAccess.lookupGuidInfo(guid)) == null) {
//        return new CommandResponse(BADRESPONSE + " " + BADGUID + " " + guid);
//      }
//      if (AccessSupport.verifySignature(guidInfo, signature, message)) {
//        AccountInfo accountInfo = AccountAccess.lookupAccountInfoFromGuid(guid);
//        if (accountInfo == null) {
//          return new CommandResponse(BADRESPONSE + " " + BADACCOUNT + " " + guid);
//        }
//        return AccountAccess.removeAlias(accountInfo, name);
//      } else {
//        return new CommandResponse(BADRESPONSE + " " + BADSIGNATURE);
//      }
    //}
  }
  
  @Override
  public String getCommandDescription() {
    return "Removes the alias from the account associated with the GUID. Must be signed by the guid. Returns "
            + BADGUID + " if the GUID has not been registered.";
    
  }
}
