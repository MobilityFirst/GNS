/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 *
 * @author westy
 */
public class ReadArrayOneUnsigned extends ReadArray {

  /**
   *
   * @param module
   */
  public ReadArrayOneUnsigned(CommandModule module) {
    super(module);
  }

  @Override
  public String getCommandName() {
    return READARRAYONE;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD};
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid. Does not require authentication but field must be set to be readable by everyone."
            + " Treats the value of key value pair as a singleton item and returns that item."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields. ";
  }
}
