/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands.data;

import edu.umass.cs.gns.clientsupport.UpdateOperation;
import static edu.umass.cs.gns.clientsupport.Defs.*;

/**
 *
 * @author westy
 */
public class ReplaceOrCreateList extends AbstractUpdateList {

  public ReplaceOrCreateList(CommandModule module) {
    super(module);
  }

  @Override
  public UpdateOperation getUpdateOperation() {
    return UpdateOperation.REPLACE_ALL_OR_CREATE;
  }

  @Override
  public String getCommandName() {
    return REPLACEORCREATELIST;
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, VALUE, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandDescription() {
    return "Adds a key value pair to the GNS for the given GUID if it does not exist otherwise "
            + "replaces the value of this key value pair for the given GUID with the value. "
            + "Value is a list of items formated as a JSON list.";
            
  }
}
