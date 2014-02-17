/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import edu.umass.cs.gns.httpserver.SystemParameter;
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
public class GetParameter extends GnsCommand {

  public GetParameter(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME};
  }

  @Override
  public String getCommandName() {
    return GETPARAMETER;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String parameterString = json.getString(NAME);
    if (module.isAdminMode()) {
      try {
        return SystemParameter.valueOf(parameterString.toUpperCase()).getFieldBoolean().toString();
      } catch (Exception e) {
        System.out.println("Problem getting parameter: " + e);
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + GETPARAMETER + " " + parameterString;
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
