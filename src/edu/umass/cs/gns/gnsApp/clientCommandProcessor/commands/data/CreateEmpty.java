/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.data;

import edu.umass.cs.gns.gnsApp.clientCommandProcessor.commands.CommandModule;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.*;

/**
 * Command that adds an empty field to the GNS for the given GUID.
 * 
 * @author westy
 */
public class CreateEmpty extends Create {

  public CreateEmpty(CommandModule module) {
    super(module);
  }

  @Override
  public String[] getCommandParameters() {
    return new String[]{GUID, FIELD, WRITER, SIGNATURE, SIGNATUREFULLMESSAGE};
  }

  @Override
  public String getCommandName() {
    return CREATE;
  }
  
  @Override
  public String getCommandDescription() {
    return "Adds an empty field to the GNS for the given GUID."
             + " Field must be writeable by the WRITER guid.";
  }
}
