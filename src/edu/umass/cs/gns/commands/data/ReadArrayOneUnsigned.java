/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.CommandModule;
import static edu.umass.cs.gns.clientsupport.Defs.*;

/**
 *
 * @author westy
 */
public class ReadArrayOneUnsigned extends ReadArray {

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
