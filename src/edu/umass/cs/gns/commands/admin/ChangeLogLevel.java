/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.admin;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class ChangeLogLevel extends GnsCommand {

  public ChangeLogLevel(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{LEVEL};
  }

  @Override
  public String getCommandName() {
    return CHANGELOGLEVEL;
  }

  @Override
  @SuppressWarnings("unchecked")
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException {
    String levelString = json.getString(LEVEL);
    if (module.isAdminMode()) {
      try {
        Level level = Level.parse(levelString);
        if (handler.getAdmintercessor().sendChangeLogLevel(level, handler)) {
          return new CommandResponse(OKRESPONSE);
        } else {
          return new CommandResponse(BADRESPONSE);
        }
      } catch (IllegalArgumentException e) {
        return new CommandResponse(BADRESPONSE + " " + GENERICERROR + " Bad level " + levelString);
      }
    }
    return new CommandResponse(BADRESPONSE + " " + OPERATIONNOTSUPPORTED + " Don't understand " + getCommandName());
  }

  @Override
  public String getCommandDescription() {
    return "Changes the log level.";
  }
}
