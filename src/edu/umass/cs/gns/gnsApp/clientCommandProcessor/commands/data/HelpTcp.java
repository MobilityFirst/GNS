/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;;

/**
 *
 * @author westy
 */
public class HelpTcp extends Help {

  public HelpTcp(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{"tcp"};
  }

  @Override
  public String getCommandName() {
    return HELP;
  }

  @Override
  public String getCommandDescription() {
    return "Returns the help message for TCP commands";
  }
}
