/*
 * Copyright (C) 2016
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.text.ParseException;

import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public interface CommandInterface {

  /**
   *
   * @return the command type
   */
  public CommandType getCommandType();

  /**
   *
   * @return the command parameters
   */
  public String[] getCommandRequiredParameters();
  
  /**
   *
   * @return the command parameters
   */
  public String[] getCommandOptionalParameters();

  /**
   *
   * @return the command description
   */
  public String getCommandDescription();

  /**
   * Executes the command. Arguments are passed in the JSONObject.
   *
   * @param commandPacket
   * @param handler
   * @return the command response of the commands
   * @throws InvalidKeyException
   * @throws InvalidKeySpecException
   * @throws JSONException
   * @throws NoSuchAlgorithmException
   * @throws SignatureException
   * @throws java.io.UnsupportedEncodingException
   * @throws java.text.ParseException
 * @throws InternalRequestException 
   */
  public CommandResponse execute(CommandPacket commandPacket, ClientRequestHandlerInterface handler) throws InvalidKeyException, InvalidKeySpecException,
          JSONException, NoSuchAlgorithmException, SignatureException,
          UnsupportedEncodingException, ParseException, InternalRequestException;

  /**
   *
   * Executes the command. Arguments are passed in the JSONObject.
   * This is used by Read and Update queries to drag {@link edu.umass.cs.gnscommon.packets.CommandPacket} for longer to use
   * {@link InternalRequestHeader} information inside them.
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
 * @throws InternalRequestException 
   */
  public CommandResponse execute(InternalRequestHeader internalHeader, CommandPacket commandPacket,
          ClientRequestHandlerInterface handler) throws InvalidKeyException,
          InvalidKeySpecException, JSONException, NoSuchAlgorithmException,
          SignatureException, UnsupportedEncodingException, ParseException, InternalRequestException;

}
