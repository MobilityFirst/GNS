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
public class SetParameter extends GnsCommand {

  public SetParameter(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, VALUE};
  }

  @Override
  public String getCommandName() {
    return SETPARAMETER;
  }

  @Override
  public String execute(JSONObject json) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String parameterString = json.getString(NAME);
    String value = json.getString(VALUE);
    if (module.isAdminMode()) {
      try {
        SystemParameter.valueOf(parameterString.toUpperCase()).setFieldBoolean(Boolean.parseBoolean(value));
        return OKRESPONSE;
      } catch (Exception e) {
        System.out.println("Problem setting parameter: " + e);
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + SETPARAMETER + " " + parameterString + " " + VALUE + " " + value;
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
