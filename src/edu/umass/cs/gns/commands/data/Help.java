/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.GnsCommand;
import edu.umass.cs.gns.commands.CommandModule;
import static edu.umass.cs.gns.clientsupport.Defs.*;
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
