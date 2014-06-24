/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */

package edu.umass.cs.aws.networktools;

import java.io.File;

/**
 *
 * @author westy
 */
public class ExecuteBash {
  
  private static final String CHMODCOMMAND = "chmod ugo+x";
  private static final String FILESEPARATOR = System.getProperty("file.separator");
  
  private static String ec2Username = "ec2-user";

  /**
   * Write a script file and execute it.
   * 
   * @param host
   * @param keyFile
   * @param withSudo
   * @param scriptName
   * @param command
   * @param arguments 
   */
  public static void executeBashScript(String host, File keyFile, boolean withSudo, String scriptName, String command, Object... arguments) {
    SSHClient.execWithSudoNoPass(ec2Username, host, keyFile, "echo \"" + command + "\" > " + scriptName);
    SSHClient.execWithSudoNoPass(ec2Username, host, keyFile, CHMODCOMMAND + " " + scriptName);
    StringBuilder argumentList = new StringBuilder();
    for (Object arg : arguments) {
      argumentList.append(" ");
      argumentList.append(arg.toString());
    }
    SSHClient.exec(ec2Username, host, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString(), withSudo, null);
  }

  /**
   * Write a script file and execute it.
   *
   * @param host
   * @param keyFile
   */
  public static void executeBashScript(String host, File keyFile, String scriptName, String command, Object... arguments) {
    executeBashScript(host, keyFile, false, scriptName, command, arguments);
  }

  public static String getEc2Username() {
    return ec2Username;
  }

  public static void setEc2Username(String ec2Username) {
    ExecuteBash.ec2Username = ec2Username;
  }
  
}
