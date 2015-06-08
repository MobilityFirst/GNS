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
import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.UpdateOperation;

/**
 *
 * @author westy
 */
public class SetFieldNullSelf extends AbstractUpdate {

  public SetFieldNullSelf(CommandModule module) {
    super(module);
  }

  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.SINGLE_FIELD_SET_FIELD_NULL;
  }

  @Override
  public String getCommandName() {
    return SETFIELDNULL;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Sets the field to contain a null value.";
            
  }
}
