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

import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gigapaxos.interfaces.Summarizable;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import static edu.umass.cs.gnsserver.httpserver.Defs.KEYSEP;
import static edu.umass.cs.gnsserver.httpserver.Defs.QUERYPREFIX;
import static edu.umass.cs.gnsserver.httpserver.Defs.VALSEP;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;

import org.json.JSONException;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.packets.CommandPacket;

/**
 * This class helps to implement a unified set of client support commands that translate
 * between client support requests and core GNS commands that are sent to the server.
 * Specifically the AbstractCommand is the superclass for all other commands.
 * It supports command sorting to facilitate command lookup. It also supports command documentation.
 *
 * @author westy, arun
 */
public abstract class AbstractCommand implements CommandInterface, Comparable<AbstractCommand>, Summarizable {

  /**
   *
   */
  protected CommandModule module;

  /**
   * Creates a new <code>AbstractCommand</code> object
   *
   * @param module
   */
  public AbstractCommand(CommandModule module) {
    this.module = module;
  }

  /**
   * Supports command sorting to facilitate command lookup.
   * We need to sort the commands to put the longer ones with the same command name first.
   *
   * @param otherCommand
   * @return an int
   */
  // 
  @Override
  public int compareTo(AbstractCommand otherCommand) {
    int alphaResult = getCommandType().toString().compareTo(otherCommand.getCommandType().toString());
    // sort by number of arguments putting the longer ones first because we need to do longest match first.
    if (alphaResult == 0) {
      int lengthDifference = getCommandRequiredParameters().length - otherCommand.getCommandRequiredParameters().length;
      if (lengthDifference != 0) {
        // longest should be "less than"
        return -(Integer.signum(lengthDifference));
      } else {
        // same length parameter strings just sort them alphabetically... they can't be equal
        return getCommandParametersString().compareTo(otherCommand.getCommandParametersString());
      }
    } else {
      return alphaResult;
    }
  }

  /**
   * Returns a string array with names of the argument parameters to the command.
   *
   * @return argument parameters
   */
  @Override
  public String[] getCommandRequiredParameters() {
    return getCommandType().getCommandRequiredParameters();
  }

  /**
   * Returns a string array with names of the argument parameters to the command.
   *
   * @return argument parameters
   */
  @Override
  public String[] getCommandOptionalParameters() {
    return getCommandType().getCommandOptionalParameters();
  }

  /**
   * Get the description of the command
   *
   * @return <code>String</code> of the command description
   */
  @Override
  public String getCommandDescription() {
    return getCommandType().getCommandDescription();
  }

  /**
   *
   * Executes the command.
   * Arguments are passed in the CommandPacket.
   * This is used by Read and Update queries to drag {@link edu.umass.cs.gnscommon.packets.CommandPacket} 
   * for longer to use {@link InternalRequestHeader} information inside them.
   *
   * @param internalHeader
   * @param commandPacket
   * @param handler
   * @return Result of executing {@code commandPacket}
   * @throws InvalidKeyException
   * @throws InvalidKeySpecException
   * @throws JSONException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws UnsupportedEncodingException
   * @throws ParseException
   * @throws edu.umass.cs.gnscommon.exceptions.server.InternalRequestException
   */
  @Override
  abstract public CommandResponse execute(InternalRequestHeader internalHeader, 
          CommandPacket commandPacket,
          ClientRequestHandlerInterface handler) throws InvalidKeyException,
          InvalidKeySpecException, JSONException, NoSuchAlgorithmException,
          SignatureException, UnsupportedEncodingException, ParseException, InternalRequestException;

  /**
   * Get the usage of the command.
   *
   * @param format
   * @return <code>String</code> of the command usage ()
   */
  public String getUsage(CommandDescriptionFormat format) {
    switch (format) {
      case HTML:
        return "HTML Form: " + getHTMLForm() + GNSProtocol.NEWLINE.toString()
                + getCommandDescription();
      case TCP:
        return getTCPForm() + GNSProtocol.NEWLINE.toString() + getCommandDescription();
      case TCP_Wiki:
        return getTCPWikiForm() + " ||" + getCommandDescription();
      default:
        return "Unknown command description format!";
    }
  }

  /**
   * Returns a string showing the HTML client usage of the command.
   *
   * @return the HTML as a string
   */
  private String getHTMLForm() {
    StringBuilder result = new StringBuilder();
    // write out lower case because we except any case
    result.append(getCommandType().toString().toLowerCase());
    String[] parameters = getCommandRequiredParameters();
    String prefix = QUERYPREFIX;
    for (int i = 0; i < parameters.length; i++) {
      // special case to remove GNSProtocol.SIGNATUREFULLMESSAGE.toString() which isn't for HTML form
      if (!GNSProtocol.SIGNATUREFULLMESSAGE.toString().equals(parameters[i])) {
        result.append(prefix);
        result.append(parameters[i]);
        result.append(VALSEP);
        result.append("<" + parameters[i] + ">");
        prefix = KEYSEP;
      }
    }

    String[] optionalParameters = getCommandOptionalParameters();
    if (optionalParameters.length > 0) {
      result.append(" additional optional parameters: ");
      prefix = "";
      for (int i = 0; i < optionalParameters.length; i++) {
        // special case to remove GNSProtocol.SIGNATUREFULLMESSAGE.toString() which isn't for HTML form
        if (!GNSProtocol.SIGNATUREFULLMESSAGE.toString().equals(optionalParameters[i])) {
          result.append(prefix);
          result.append(optionalParameters[i]);
          result.append(VALSEP);
          result.append("<" + optionalParameters[i] + ">");
          prefix = KEYSEP;
        }
      }
    }
    return result.toString();
  }

  /**
   * Returns a string showing the TCP client usage of the command.
   *
   * @return the doc as a string
   */
  private String getTCPForm() {
    StringBuilder result = new StringBuilder();
    result.append("Command: ");
    result.append(getCommandType().toString());
    String[] parameters = getCommandRequiredParameters();
    result.append(" Required Parameters: ");
    String prefix = "";
    for (String parameter : parameters) {
      if (!GNSProtocol.SIGNATUREFULLMESSAGE.toString().equals(parameter)) {
        result.append(prefix);
        result.append(parameter);
        prefix = ", ";
      }
    }
    String[] optionalParameters = getCommandOptionalParameters();
    if (optionalParameters.length > 0) {
      result.append(" Optional Parameters: ");
      prefix = "";
      for (String parameter : optionalParameters) {
        if (!GNSProtocol.SIGNATUREFULLMESSAGE.toString().equals(parameter)) {
          result.append(prefix);
          result.append(parameter);
          prefix = ", ";
        }
      }
    }
    return result.toString();
  }

  /**
   * Outputs the command information in a format that can be used on a Media Wiki page.
   *
   * @return the wiki string
   */
  private String getTCPWikiForm() {
    StringBuilder result = new StringBuilder();
    result.append("|- "); // start row
    result.append(GNSProtocol.NEWLINE.toString());
    result.append("|");
    result.append(getCommandType().toString());
    String[] parameters = getCommandRequiredParameters();
    result.append(" || ");
    String prefix = "";
    for (int i = 0; i < parameters.length; i++) {
      if (!GNSProtocol.SIGNATUREFULLMESSAGE.toString().equals(parameters[i])) {
        result.append(prefix);
        result.append(parameters[i]);
        prefix = ", ";
      }
    }
    String[] optionalParamaters = getCommandOptionalParameters();
    result.append(" || ");
    prefix = "";
    for (int i = 0; i < optionalParamaters.length; i++) {
      if (!GNSProtocol.SIGNATUREFULLMESSAGE.toString().equals(optionalParamaters[i])) {
        result.append(prefix);
        result.append(optionalParamaters[i]);
        prefix = ", ";
      }
    }
    return result.toString();
  }

  /**
   * Returns a string describing the parameters of the command.
   *
   * @return the parameters as a string
   */
  public String getCommandParametersString() {
    StringBuilder result = new StringBuilder();
    String[] parameters = getCommandRequiredParameters();
    String prefix = "";
    for (int i = 0; i < parameters.length; i++) {
      result.append(prefix);
      result.append(parameters[i]);
      prefix = ",";
    }
    return result.toString();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName()
            + ":" + getCommandType().toString()
            + ":" + getCommandType().getInt() + " ["
            + getCommandParametersString() + "]";
  }

  /**
   *
   * @return the summary
   */
  @Override
  public Object getSummary() {
    return new Object() {
      @Override
      public String toString() {
        return AbstractCommand.this.toString();
      }
    };
  }
}
