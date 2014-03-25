/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.clientsupport.Admintercessor;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.commands.data.CommandModule;
import edu.umass.cs.gns.commands.data.GnsCommand;
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
public class DeleteAllRecords extends GnsCommand {

  public DeleteAllRecords(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{};
  }

  @Override
  public String getCommandName() {
    return DELETEALLRECORDS;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    if (module.isAdminMode()) {
      if (Admintercessor.sendDeleteAllRecords()) {
        return OKRESPONSE;
      } else {
        return BADRESPONSE;
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName();
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Deletes all records.";
  }
}
