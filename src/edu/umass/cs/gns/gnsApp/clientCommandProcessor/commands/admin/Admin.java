/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.admin;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
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
public class Admin extends GnsCommand {
  
  /**
   *
   * @param module
   */
  public Admin(CommandModule module) {
    super(module);
  }
  
  @Override
  public String[] getCommandParameters() {
    return new String[]{PASSKEY};
  }
  
  @Override
  public String getCommandName() {
    return ADMIN;
  }
  
  @Override
  public CommandResponse<String> execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String passkey = json.getString(PASSKEY);
    if (module.getHTTPHost().equals(passkey)) {
      module.setAdminMode(true);
      return new CommandResponse<String>(OKRESPONSE);
    } else if ("off".equals(passkey)) {
      module.setAdminMode(false);
      return new CommandResponse<String>(OKRESPONSE);
    }
    return new CommandResponse<String>(BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName() + " " + passkey);
  }
  
  @Override
  public String getCommandDescription() {
    return "Turns on admin mode.";
  }
}
