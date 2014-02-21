/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.client.Admintercessor;
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
public class ResetDatabase extends GnsCommand {

  public ResetDatabase(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{};
  }

  @Override
  public String getCommandName() {
    return RESETDATABASE;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    if (module.isAdminMode()) {
      if (Admintercessor.sendResetDB()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName();
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Rests the GNS to an initialized state. The nuclear option.";
  }
}
