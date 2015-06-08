/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 *
 * @author westy
 */
public class ReadArrayOneSelf extends ReadArray {

  public ReadArrayOneSelf(CommandModule module) {
    super(module);
  }
  
  @Override
  public String getCommandName() {
    return READARRAYONE;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that GUID making request has access authority. "
            + "Treats the value of key value pair as a singleton item and returns that item."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields. ";
  }
}
