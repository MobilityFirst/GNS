/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

import static edu.umass.cs.gns.clientprotocol.Defs.*;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Help extends GnsCommand {

  public Help(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{};
  }

  @Override
  public String getCommandName() {
    return HELP;
  }

  @Override
  public String execute(JSONObject json) {
    return "Commands are sent as HTTP GET queries." + NEWLINE + NEWLINE
            + "Note: We use the terms field and key interchangably below." + NEWLINE + NEWLINE
            + "Commands:" + NEWLINE
            + module.allCommandDescriptionsForHTML();
  }

  @Override
  public String getCommandDescription() {
    return "Returns this help message";
  }
}
