/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

/**
 *
 * @author westy
 */
public class CommandDefs {

  private static String[] commands = new String[]{
    "edu.umass.cs.gns.commands.ReadSelf",
    "edu.umass.cs.gns.commands.Read",
    "edu.umass.cs.gns.commands.ReadUnsigned",
    "edu.umass.cs.gns.commands.ReadOneSelf",
    "edu.umass.cs.gns.commands.ReadOne",
    "edu.umass.cs.gns.commands.ReadOneUnsigned",
    "edu.umass.cs.gns.commands.Create",
    "edu.umass.cs.gns.commands.CreateSelf",
    "edu.umass.cs.gns.commands.CreateEmpty",
    "edu.umass.cs.gns.commands.CreateEmptySelf",
    "edu.umass.cs.gns.commands.CreateList",
    "edu.umass.cs.gns.commands.CreateListSelf",
    "edu.umass.cs.gns.commands.RegisterAccount",
    "edu.umass.cs.gns.commands.RegisterAccountWithoutGuid",
    "edu.umass.cs.gns.commands.RegisterAccountWithoutPassword",
    "edu.umass.cs.gns.commands.RegisterAccountWithoutGuidOrPassword"
  };

  public static String[] getCommandDefs() {
    return commands;
  }
}
