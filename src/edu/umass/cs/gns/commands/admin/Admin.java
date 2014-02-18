/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
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
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String passkey = json.getString(PASSKEY);
    if (module.getHost().equals(passkey)) {
      module.setAdminMode(true);
      return OKRESPONSE;
    } else if ("off".equals(passkey)) {
      module.setAdminMode(false);
      return OKRESPONSE;
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName() + " " + passkey;
  }
  
  @Override
  public String getCommandDescription() {
    return "Turns on admin mode.";
  }
}
