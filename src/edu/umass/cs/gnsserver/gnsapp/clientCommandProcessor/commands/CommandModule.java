/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands;

import org.json.JSONException;
import org.json.JSONObject;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.TreeSet;
import static edu.umass.cs.gnscommon.GNSCommandProtocol.*;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientCommandProcessorConfig;
import edu.umass.cs.gnsserver.gnsapp.clientSupport.ClientSupportConfig;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * This class helps to implement a unified set of client support commands that translate
 * between client support requests and core GNS commands that are sent to the server.
 * Specifically the CommandModule class maintains the list of commands, mechanisms
 * for looking up commands from the contents of JSONObject encoded command packets
 * as well as supporting generation of command documentation.
 *
 * @author westy
 */
public class CommandModule {

  private Map<CommandType, BasicCommand> commandLookupTable;

  public void addCommand(CommandType commandType, BasicCommand command) {
    if (commandLookupTable.get(commandType) != null) {
      ClientSupportConfig.getLogger().log(Level.SEVERE,
              "Duplicate command: {0}", commandType);
    }
    commandLookupTable.put(commandType, command);
  }

  private TreeSet<BasicCommand> commands;
  private boolean adminMode = false;

  /**
   * Creates a CommandModule.
   */
  public CommandModule() {
    initCommands();
  }

  private void initCommands() {
    commandLookupTable = new HashMap<>();
    // Legacy code
    this.commands = new TreeSet<>();
    addCommands(CommandDefs.getCommandDefs(), commands);
    ClientCommandProcessorConfig.getLogger().log(Level.INFO,
            "{0} commands added.", commands.size());
  }

  /**
   *
   * Add commands to this module. Commands instances are created by reflection
   * based on the command class names passed in parameter
   *
   * @param commandClasses a String[] containing the class names of the command
   * to instantiate
   * @param commands Set where the commands are added
   */
  protected void addCommands(Class<?>[] commandClasses, Set<BasicCommand> commands) {
    for (int i = 0; i < commandClasses.length; i++) {
      Class<?> clazz = commandClasses[i];
      BasicCommand command = createCommandInstance(clazz);
      if (command != null) {
        commandLookupTable.put(command.getCommandType(), command);
        // Legacy
        commands.add(command);
      }
    }
  }

  private BasicCommand createCommandInstance(Class<?> clazz) {
    try {
      Constructor<?> constructor;
      try {
        constructor = clazz.getConstructor(new Class<?>[]{this.getClass()});
      } catch (NoSuchMethodException e) {
        constructor = clazz.getConstructor(new Class<?>[]{CommandModule.class});
      }
      BasicCommand command = (BasicCommand) constructor.newInstance(new Object[]{this});
      ClientCommandProcessorConfig.getLogger().log(Level.FINE,
              "Creating command {0}: {1} with {2}: {3}",
              new Object[]{command.getCommandType().getInt(), clazz.getCanonicalName(), command.getCommandName(),
                command.getCommandParametersString()});
      return command;
    } catch (SecurityException | NoSuchMethodException |
            InstantiationException | IllegalAccessException |
            IllegalArgumentException | InvocationTargetException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
              "Unable to create command for class {0}: {1}",
              new Object[]{clazz.getCanonicalName(), e});
    }
    return null;
  }

  /**
   * Finds the command that corresponds to the JSONObject which was received command packet.
   *
   * @param json
   * @return
   */
  public BasicCommand lookupCommand(JSONObject json) {
    BasicCommand command = null;
    if (json.has(COMMAND_INT)) {
      try {
        command = commandLookupTable.get(CommandType.getCommandType(json.getInt(COMMAND_INT)));
        // Some sanity checks
        String commandName = json.optString(COMMANDNAME, null);
        // Check to see if command name
        if (command != null && commandName != null && !commandName.equals(command.getCommandName())) {
          ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                  "Command name {0} in json does not match {1}",
                  new Object[]{commandName, command.getCommandName()});
          command = null;
        }
        if (command != null && !JSONContains(json, command.getCommandParameters())) {
          ClientCommandProcessorConfig.getLogger().log(Level.SEVERE,
                  "For {0} missing parameter {1}",
                  new Object[]{commandName, JSONMissing(json, command.getCommandParameters())});
          command = null;
        }
      } catch (JSONException e) {
        // do nothing
      }
    }
    if (command != null) {
      ClientCommandProcessorConfig.getLogger().log(Level.FINE,
              "Found {0} using table lookup", command);
      return command;
    }
    // Keep the old method for backward compatibility with older clients that
    // aren't using the COMMAND_INT field
    return lookupCommandLinearSearch(json);
  }

  public BasicCommand lookupCommandLinearSearch(JSONObject json) {
    String action;
    try {
      action = json.getString(COMMANDNAME);
    } catch (JSONException e) {
      ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
              "Unable find " + COMMANDNAME + " key in JSON command: {0}", e);
      return null;
    }
    ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
            "Linear search of {0} commands:", commands.size());
    // for now a linear search is fine
    for (BasicCommand lookupCommand : commands) {
      //GNS.getLogger().info("Search: " + command.toString());
      if (lookupCommand.getCommandName().equals(action)) {
        //GNS.getLogger().info("Found action: " + action);
        if (JSONContains(json, lookupCommand.getCommandParameters())) {
          //GNS.getLogger().info("Matched parameters: " + json);
          return lookupCommand;
        }
      }
    }
    ClientCommandProcessorConfig.getLogger().log(Level.WARNING,
            "***COMMAND SEARCH***: Unable to find {0}", json);
    return null;
  }

  /**
   *
   */
  public static final String STANDARD_PREAMBLE = "COMMAND PACKAGE: %s";

  /**
   *
   */
  public static final String WIKI_PREAMBLE = "{| class=\"wikitable\"\n"
          + "|+ Commands in %s\n"
          + "! scope=\"col\" | Command Name\n"
          + "! scope=\"col\" | Parameters\n"
          + "! scope=\"col\" | Description";

  /**
   * Return all the command descriptions.
   *
   * @param format
   * @return a string
   */
  public String allCommandDescriptions(CommandDescriptionFormat format) {
    StringBuilder result = new StringBuilder();
    List<BasicCommand> commandList = new ArrayList<>(commands);
    // First sort by name
    Collections.sort(commandList, CommandNameComparator);
    // The sort them by package
    Collections.sort(commandList, CommandPackageComparator);
    String lastPackageName = null;
    for (BasicCommand command : commandList) {
      String packageName = command.getClass().getPackage().getName();
      if (!packageName.equals(lastPackageName)) {
        if (format.equals(CommandDescriptionFormat.TCP_Wiki) && lastPackageName != null) {
          // finish last table
          result.append("|}");
        }
        lastPackageName = packageName;
        result.append(NEWLINE);
        result.append(String.format(format.equals(CommandDescriptionFormat.TCP_Wiki)
                ? WIKI_PREAMBLE : STANDARD_PREAMBLE, lastPackageName));
        result.append(NEWLINE);
      }
      //result.append(NEWLINE);
      //result.append(cnt++ + ": ");
      result.append(command.getUsage(format));
      result.append(NEWLINE);
      result.append(NEWLINE);
    }
    return result.toString();
  }

  private String JSONMissing(JSONObject json, String[] parameters) {
    for (int i = 0; i < parameters.length; i++) {
      if (json.optString(parameters[i], null) == null) {
        return parameters[i];
      }
    }
    return null;
  }

  private boolean JSONContains(JSONObject json, String[] parameters) {
    return JSONMissing(json, parameters) == null;
  }

  /**
   * Return true if we are in admin mode.
   *
   * @return true if we are in admin mode
   */
  public boolean isAdminMode() {
    return adminMode;
  }

  /**
   * Set admin mode.
   *
   * @param adminMode
   */
  public void setAdminMode(boolean adminMode) {
    this.adminMode = adminMode;
  }

  private static Comparator<BasicCommand> CommandPackageComparator
          = new Comparator<BasicCommand>() {

    @Override
    public int compare(BasicCommand command1, BasicCommand command2) {

      String packageName1 = command1.getClass().getPackage().getName();
      String packageName2 = command2.getClass().getPackage().getName();

      //ascending order
      return packageName1.compareTo(packageName2);

      //descending order
      //return fruitName2.compareTo(fruitName1);
    }

  };

  /**
   *
   */
  private static Comparator<BasicCommand> CommandNameComparator
          = new Comparator<BasicCommand>() {

    @Override
    public int compare(BasicCommand command1, BasicCommand command2) {

      String commandName1 = command1.getCommandName();
      String commandName2 = command2.getCommandName();

      //ascending order
      return commandName1.compareTo(commandName2);

      //descending order
      //return fruitName2.compareTo(fruitName1);
    }

  };
}
