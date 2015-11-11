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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commands;

import edu.umass.cs.gnsserver.main.GNS;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.TreeSet;

import static edu.umass.cs.gnscommon.GnsProtocol.COMMANDNAME;
import static edu.umass.cs.gnscommon.GnsProtocol.NEWLINE;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This class helps to implement a unified set of client support commands that translate between client support requests
 * and core GNS commands that are sent to the server. Specifically the CommandModule class maintains the list
 * of commands, mechanisms for looking up commands from the contents of JSONObject encoded command packets
 * as well as supporting generation of command documentation.
 * 
 * @author westy
 */
public class CommandModule {

  private TreeSet<GnsCommand> commands;
  private String httpHost;
  private boolean adminMode = false;

  /**
   * Creates a CommandModule.
   */
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
   * to instantiate
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
        GNS.getLogger().fine("Adding command " + (i + 1) + ": " + commandClassName + " with " + command.getCommandName() + ": " + command.getCommandParametersString());
        commands.add(command);
      } catch (Exception e) {
        GNS.getLogger().severe("Unable to add command for class " + commandClassName + ": " + e);
      }
    }
  }

  /**
   * Finds the command that corresponds to the JSONObject which was received command packet.
   * 
   * @param json
   * @return 
   */
  public GnsCommand lookupCommand(JSONObject json) {
    String action;
    try {
      action = json.getString(COMMANDNAME);
    } catch (JSONException e) {
      GNS.getLogger().warning("Unable find " + COMMANDNAME + " key in JSON command: " + e);
      return null;
    }
    GNS.getLogger().fine("Searching " + commands.size() + " commands:");
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
  
  /**
   * The types of command description formats we support.
   */
  public enum CommandDescriptionFormat {

    /**
     *
     */
    HTML,

    /**
     *
     */
    TCP,

    /**
     *
     */
    TCP_Wiki
  }
  
  /**
   *
   */
  public static final String standardPreamble = "COMMAND PACKAGE: %s";
  
  /**
   *
   */
  public static final String wikiPreamble = "{| class=\"wikitable\"\n" +
"|+ Commands in %s\n" +
"! scope=\"col\" | Command Name\n" +
"! scope=\"col\" | Parameters\n" +
"! scope=\"col\" | Description";

  /**
   * Return all the command descriptions.
   * 
   * @param format
   * @return a string
   */
  public String allCommandDescriptions(CommandDescriptionFormat format) {
    StringBuilder result = new StringBuilder();
    List<GnsCommand> commandList = new ArrayList<>(commands);
    // First sort by name
    Collections.sort(commandList, CommandNameComparator);
    // The sort them by package
    Collections.sort(commandList, CommandPackageComparator);
    String lastPackageName = null; 
    for (GnsCommand command : commandList) {
      String packageName = command.getClass().getPackage().getName();
      if (!packageName.equals(lastPackageName)) {
         if (format.equals(CommandDescriptionFormat.TCP_Wiki) && lastPackageName != null) {
          // finish last table
          result.append("|}");
        }
        lastPackageName = packageName;
        result.append(NEWLINE);
        result.append(String.format(format.equals(CommandDescriptionFormat.TCP_Wiki) ? wikiPreamble : standardPreamble, lastPackageName));
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

  private boolean JSONContains(JSONObject json, String[] parameters) {
    for (int i = 0; i < parameters.length; i++) {
      if (json.optString(parameters[i], null) == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return the HTTPHost.
   * 
   * @return a string
   */
  public String getHTTPHost() {
    return httpHost;
  }

  /**
   * Set the HTTPHost.
   * @param host
   */
  public void setHTTPHost(String host) {
    this.httpHost = host;
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

  private static Comparator<GnsCommand> CommandPackageComparator
          = new Comparator<GnsCommand>() {

            @Override
            public int compare(GnsCommand command1, GnsCommand command2) {

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
  public static Comparator<GnsCommand> CommandNameComparator
          = new Comparator<GnsCommand>() {

            @Override
            public int compare(GnsCommand command1, GnsCommand command2) {

              String commandName1 = command1.getCommandName();
              String commandName2 = command2.getCommandName();

              //ascending order
              return commandName1.compareTo(commandName2);

              //descending order
              //return fruitName2.compareTo(fruitName1);
            }

          };
}
