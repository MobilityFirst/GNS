/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 * 
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.commands;

import edu.umass.cs.gns.clientsupport.Defs;
import static edu.umass.cs.gns.clientsupport.Defs.*;
import edu.umass.cs.gns.main.GNS;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.TreeSet;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class CommandModule {

  private TreeSet<GnsCommand> commands;
  private String host;
  private boolean adminMode = false;

  public CommandModule() {
    initCommands();
  }

  private void initCommands() {
    this.commands = new TreeSet<GnsCommand>();
    addCommands(CommandDefs.getCommandDefs(), commands);
    GNS.getLogger().info(commands.size() + " commands added.");
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
        GNS.getLogger().info("Adding command " + (i + 1) + ": " + commandClassName + " with " + command.getCommandName() + ": " + command.getCommandParametersString());
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
     GNS.getLogger().info("Searching " + commands.size() + " commands:");
    //GNS.getLogger().info("Looking for: " + json);
    // for now a linear search is fine
    for (GnsCommand command : commands) {
      //GNS.getLogger().info("Search: " + command.toString());
      if (command.getCommandName().equals(action)) {
        //GNS.getLogger().info("Found action: " + action);
        if (JSONContains(json, command.getCommandParameters())) {
          //GNS.getLogger().info("Matched parameters: " + json);
          return command;
        }
      }
    }
    GNS.getLogger().warning("***COMMAND SEARCH***: Unable to find " + json);
    return null;
  }

  public String allCommandDescriptionsForHTML() {
    StringBuffer result = new StringBuffer();
    String prefix = "";
    int cnt = 1;
    for (GnsCommand command : commands) {
      result.append(prefix);
      result.append(cnt++ + ": ");
      result.append(command.getUsage());
      prefix = Defs.NEWLINE;
      result.append(NEWLINE);
    }
    return result.toString();
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

  public boolean isAdminMode() {
    return adminMode;
  }

  public void setAdminMode(boolean adminMode) {
    this.adminMode = adminMode;
  }
}
