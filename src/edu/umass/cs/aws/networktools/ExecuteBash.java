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

  public static void executeBashScript(String dns, File keyFile, boolean withSudo, String scriptName, String command, Object... arguments) {
    SSHClient.execWithSudoNoPass(ec2Username, dns, keyFile, "echo \"" + command + "\" > " + scriptName);
    SSHClient.execWithSudoNoPass(ec2Username, dns, keyFile, CHMODCOMMAND + " " + scriptName);
    StringBuilder argumentList = new StringBuilder();
    for (Object arg : arguments) {
      argumentList.append(" ");
      argumentList.append(arg.toString());
    }
    SSHClient.exec(ec2Username, dns, keyFile, "." + FILESEPARATOR + scriptName + argumentList.toString(), withSudo, null);
  }

  /**
   * Write a script file and execute it.
   *
   * @param dns
   * @param keyFile
   */
  public static void executeBashScript(String dns, File keyFile, String scriptName, String command, Object... arguments) {
    executeBashScript(dns, keyFile, false, scriptName, command, arguments);
  }

  public static String getEc2Username() {
    return ec2Username;
  }

  public static void setEc2Username(String ec2Username) {
    ExecuteBash.ec2Username = ec2Username;
  }
  
}
