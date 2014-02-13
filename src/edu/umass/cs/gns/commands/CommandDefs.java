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

  private static String[] commands =
          new String[]{
          "edu.umass.cs.gns.commands.ReadSelf",
          "edu.umass.cs.gns.commands.Read",
          "edu.umass.cs.gns.commands.ReadUnsigned",
          "edu.umass.cs.gns.commands.ReadOneSelf",
          "edu.umass.cs.gns.commands.ReadOneWithReader",
          "edu.umass.cs.gns.commands.ReadOneUnsigned"
          };

  public static String[] getCommandDefs() {
    return commands;
  }
  
}
