
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


public abstract class AbstractCommand implements CommandInterface, Comparable<AbstractCommand>, Summarizable {


  protected CommandModule module;


  public AbstractCommand(CommandModule module) {
    this.module = module;
  }


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


  @Override
  public String[] getCommandRequiredParameters() {
    return getCommandType().getCommandRequiredParameters();
  }


  @Override
  public String[] getCommandOptionalParameters() {
    return getCommandType().getCommandOptionalParameters();
  }


  @Override
  public String getCommandDescription() {
    return getCommandType().getCommandDescription();
  }


  @Override
  abstract public CommandResponse execute(InternalRequestHeader internalHeader, 
          CommandPacket commandPacket,
          ClientRequestHandlerInterface handler) throws InvalidKeyException,
          InvalidKeySpecException, JSONException, NoSuchAlgorithmException,
          SignatureException, UnsupportedEncodingException, ParseException, InternalRequestException;


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
