/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.admin;

import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;
import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;

/**
 *
 * @author westy
 */
public class BatchTestName extends BatchTest {

  /**
   *
   * @param module
   */
  public BatchTestName(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{NAME, GUIDCNT};
  }
  
  @Override
  public String getCommandDescription() {
    return "Creates N guids using batch create for the given account which will be created if not already present."
            + " Uses a fake public key.";
  }
}
