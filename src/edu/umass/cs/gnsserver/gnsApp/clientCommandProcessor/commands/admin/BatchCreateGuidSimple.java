/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.admin;

import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;

/**
 *
 * @author westy
 */
public class BatchCreateGuidSimple extends BatchCreateGuid {

  /**
   *
   * @param module
   */
  public BatchCreateGuidSimple(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUIDCNT};
  }

  @Override
  public String getCommandDescription() {
    return "Creates N guids using batch create and a default account and fake public key.";
  }
}
