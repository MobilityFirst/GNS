/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 * 
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

import edu.umass.cs.gns.main.GNS;
import java.lang.reflect.Constructor;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.json.JSONObject;
import static edu.umass.cs.gns.clientprotocol.Defs.*;
import org.json.JSONException;

/**
 *
 * @author westy
 */
public class CommandModule {
  
  TreeSet<GnsCommand> commands;
  
  public CommandModule() {
    initCommands();
  }
  
  private void initCommands() {
    this.commands = new TreeSet<GnsCommand>();
    addCommands(CommandDefs.getCommandDefs(), commands);
  }

  /**
   * Add commands to this module. Commands instances are created by reflection
   * based on the command class names passed in parameter
   * 
   * @param commandClasses a String[] containing the class names of the command
   *          to instantiate
   * @param commands Set where the commands are added
   */
  protected void addCommands(String[] commandClasses, Set<GnsCommand> commands) {
    for (int i = 0; i < commandClasses.length; i++) {
      String commandClassName = commandClasses[i].trim();
      Class<?> clazz;
      try {
        clazz = Class.forName(commandClassName);
        Constructor<?> constructor;
        try {
          constructor = clazz.getConstructor(new Class[]{this.getClass()});
        } catch (NoSuchMethodException e) {
          constructor = clazz.getConstructor(new Class[]{CommandModule.class});
        }
        GnsCommand command = (GnsCommand) constructor.newInstance(new Object[]{this});
        GNS.getLogger().info("Adding command " + commandClassName);
        commands.add(command);
      } catch (Exception e) {
        GNS.getLogger().warning("Unable to add command for class " + commandClassName + ": " + e);
      }
    }
  }
  
  public GnsCommand lookupCommand(JSONObject json) {
    String action;
    try {
      action = json.getString(COMMANDNAME);
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable find " + COMMANDNAME + " key in JSON command: " + e);
      return null;
    }
    for (GnsCommand command : commands) {
      GNS.getLogger().info("Search: " + command.toString());
      if (command.getCommandName().equals(action) && JSONContains(json, command.getCommandParameters())) {
        return command;
      }
    }    
    return null;
  }
  
  private boolean JSONContains(JSONObject json, String[] parameters) {
    for (int i = 0; i < parameters.length; i++) {
      if (json.optString(parameters[i], null) == null) {
        return false;
      }
    }
    return true;
  }
}
