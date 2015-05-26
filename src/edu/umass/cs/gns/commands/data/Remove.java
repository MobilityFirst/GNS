/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.commands.CommandModule;
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;
import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.Defs.*;

/**
 *
 * @author westy
 */
public class Remove extends AbstractUpdate {

  public Remove(CommandModule module) {
    super(module);
  }

  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_REMOVE;
  }

  @Override
  public String getCommandName() {
    return REMOVE;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Returns one key value pair from the GNS for the given guid after authenticating that WRITER making request has access authority."
            + " Values are always returned as a JSON list."
            + " Specify " + ALLFIELDS + " as the <field> to return all fields as a JSON object.";
  }
}
