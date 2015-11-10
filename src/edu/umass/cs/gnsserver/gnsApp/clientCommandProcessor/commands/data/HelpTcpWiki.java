/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;;

/**
 *
 * @author westy
 */
public class HelpTcpWiki extends Help {

  /**
   *
   * @param module
   */
  public HelpTcpWiki(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{"tcpwiki"};
  }

  @Override
  public String getCommandName() {
    return HELP;
  }

  @Override
  public String getCommandDescription() {
    return "Returns the help message for TCP commands in wiki format";
  }
}
