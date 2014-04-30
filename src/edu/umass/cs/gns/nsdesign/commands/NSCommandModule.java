/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 * 
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.nsdesign.commands;

import edu.umass.cs.gns.main.GNS;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.TreeSet;

import static edu.umass.cs.gns.clientsupport.Defs.COMMANDNAME;

/**
 *
 * @author westy
 */
public class NSCommandModule {

  private TreeSet<NSCommand> commands;
  private String host;

  public NSCommandModule() {
    initCommands();
  }

  private void initCommands() {
    this.commands = new TreeSet<NSCommand>();
    addCommands(NSCommandDefs.getCommandDefs(), commands);
    GNS.getLogger().info(commands.size() + " commands added.");
  }

  /**
   * Add commands to this module. Commands instances are created by reflection
   * based on the command class names passed in parameter
   *
   * @param commandClasses a String[] containing the class names of the command
   * to instantiate
   * @param commands Set where the commands are added
   */
  protected void addCommands(String[] commandClasses, Set<NSCommand> commands) {
    for (int i = 0; i < commandClasses.length; i++) {
      String commandClassName = commandClasses[i].trim();
      Class<?> clazz;
      try {
        clazz = Class.forName(commandClassName);
        Constructor<?> constructor;
        try {
          constructor = clazz.getConstructor(new Class[]{this.getClass()});
        } catch (NoSuchMethodException e) {
          constructor = clazz.getConstructor(new Class[]{NSCommandModule.class});
        }
        NSCommand command = (NSCommand) constructor.newInstance(new Object[]{this});
        GNS.getLogger().info("Adding command " + (i + 1) + ": " + commandClassName + " with " + command.getCommandName() + ": " + command.getCommandParametersString());
        commands.add(command);
      } catch (Exception e) {
        GNS.getLogger().severe("Unable to add command for class " + commandClassName + ": " + e);
      }
    }
  }

  public NSCommand lookupCommand(JSONObject json) {
    String action;
    try {
      action = json.getString(COMMANDNAME);
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable find " + COMMANDNAME + " key in JSON command: " + e);
      return null;
    }
    GNS.getLogger().fine("Searching " + commands.size() + " commands:");
    // for now a linear search is fine
    for (NSCommand command : commands) {
      //GNS.getLogger().info("Search: " + command.toString());
      if (command.getCommandName().equals(action)) {
        //GNS.getLogger().info("Found action: " + action);
        if (JSONContains(json, command.getCommandParameters())) {
          //GNS.getLogger().info("Matched parameters: " + json);
          return command;
        }
      }
    }
    GNS.getLogger().warning("*** NS COMMAND SEARCH ***: Unable to find " + json);
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

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

}
