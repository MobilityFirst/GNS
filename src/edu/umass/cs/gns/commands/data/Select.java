/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.FieldAccess;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A basic query that returns all guids that have a field with the given value.
 * 
 * @author westy
 */
public class Select extends GnsCommand {

  public Select(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{FIELD, VALUE};
  }

  @Override
  public String getCommandName() {
    return SELECT;
  }

  @Override
  public CommandResponse execute(JSONObject json, ClientRequestHandlerInterface handler) throws JSONException {
    String field = json.getString(FIELD);
    String value = json.getString(VALUE);
    return FieldAccess.select(field, value, handler);
  }

  @Override
  public String getCommandDescription() {
    return "Returns all records that have a field with the given value.";
  }
}
