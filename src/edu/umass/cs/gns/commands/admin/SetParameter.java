/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.clientsupport.SystemParameter;
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
        SystemParameter.valueOf(parameterString.toUpperCase()).setFieldValue(value);
        return OKRESPONSE;
      } catch (Exception e) {
        System.out.println("Problem setting parameter: " + e);
      }
    }
    return BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + SETPARAMETER + " " + parameterString + " " + VALUE + " " + value;
  }

  @Override
  public String getCommandDescription() {
    return "[ONLY IN ADMIN MODE] Changes a parameter value.";
  }
}
