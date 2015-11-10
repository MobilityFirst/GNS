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
package edu.umass.cs.aws.networktools;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import edu.umass.cs.gnsserver.main.GNS;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;

/**
 * Implements SSH execute and copy commands
 *
 * @author westy
 */
public class SSHClient {
  
  private static boolean verbose = true;

  public static void exec() {
    exec(null, null, null, null, false, null);
  }

  public static void exec(String command) {
    exec(null, null, null, command, false, null);
  }

  public static void exec(String user, String host, File keyFile, String command) {
    exec(user, host, keyFile, command, false, null);
  }

  public static void execWithSudoNoPass(String user, String host, File keyFile, String command) {
    exec(user, host, keyFile, command, true, null);
  }
  public static final int MAXCOMMANDBYTES = 4096;

  public static void exec(String user, String host, File keyFile, String command, boolean useSudo, String sudoPasswd) {
    if (verbose) {
      System.out.println("Remote execute command on " + host + (useSudo ? " as root user: " : " as user " + user + ": ") + command);
    }
    try {
      JSch jsch = new JSch();

      Session session = authenticateWithKey(jsch, user, host, keyFile);

      // username and password will be given via UserInfo interface.
      UserInfo ui = new UserInfoPrompted();
      session.setUserInfo(ui);
      session.connect();

      if (command == null) {
        command = JOptionPane.showInputDialog("Enter command",
                "set|grep SSH");
      }

      Channel channel = session.openChannel("exec");
      //((ChannelExec) channel).setCommand(command);

      if (useSudo && sudoPasswd != null) {
        ((ChannelExec) channel).setCommand("sudo -S -p '' " + command);
      } else if (useSudo) {
        ((ChannelExec) channel).setCommand("sudo -p '' " + command);
      } else {
        ((ChannelExec) channel).setCommand(command);
      }
//      if (useSudo) {
//        ((ChannelExec) channel).setCommand("sudo -S -p '' " + command);
//      } else {
//        ((ChannelExec) channel).setCommand(command);
//      }

      // ??? NOT SURE WHY THIS IS HERE
      channel.setInputStream(null);

      InputStream in = channel.getInputStream();

      // prep the out channel so we can give a password if it is needed
      OutputStream out = null;
      if (useSudo && sudoPasswd != null) {
        out = channel.getOutputStream();
      }

      ((ChannelExec) channel).setErrStream(System.err);

      // WESTY ADDED THIS *****
      if (useSudo) {
        ((ChannelExec) channel).setPty(true); // fixes "you must have a tty to sudo" problem
      }
      channel.connect();

      // suppply the password for sudo
      if (out != null) {
        out.write((sudoPasswd + "\n").getBytes());
        out.flush();
      }

      byte[] tmp = new byte[MAXCOMMANDBYTES];
      while (true) {
        while (in.available() > 0) {
          int i = in.read(tmp, 0, MAXCOMMANDBYTES);
          if (i < 0) {
            break;
          }
          if (!verbose) {
            System.out.print("o");
          } else {
            System.out.print(new String(tmp, 0, i));
          }
        }
        if (channel.isClosed()) {
          GNS.getLogger().fine("exit status: " + channel.getExitStatus());
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (Exception ee) {
        }
      }
      channel.disconnect();
      session.disconnect();
    } catch (Exception e) {
      e.printStackTrace();
      GNS.getLogger().severe(e.toString());
    }
  }

  public static void scpTo(String user, String host, File keyFile, String lfile, String rfile) {
    if (verbose) {
      System.out.println("Remote copy file from " + lfile + " to " + host + "@" + user + ":" + rfile);
    }
    FileInputStream fis = null;
    try {

      JSch jsch = new JSch();
      Session session = authenticateWithKey(jsch, user, host, keyFile);

      // username and password will be given via UserInfo interface.
      UserInfo ui = new UserInfoPrompted();
      session.setUserInfo(ui);
      session.connect();

      boolean ptimestamp = true;

      // exec 'scp -t rfile' remotely
      String command = "scp " + (ptimestamp ? "-p" : "") + " -t " + rfile;
      Channel channel = session.openChannel("exec");
      ((ChannelExec) channel).setCommand(command);

      // get I/O streams for remote scp
      OutputStream out = channel.getOutputStream();
      InputStream in = channel.getInputStream();

      channel.connect();

      int ack = checkAck(in);
      if (ack != 0) {
        System.out.println("ACK was " + ack);
        return;
      }

      File _lfile = new File(lfile);

      if (ptimestamp) {
        command = "T " + (_lfile.lastModified() / 1000) + " 0";
        // The access time should be sent here,
        // but it is not accessible with JavaAPI ;-<
        command += (" " + (_lfile.lastModified() / 1000) + " 0\n");
        out.write(command.getBytes());
        out.flush();
        ack = checkAck(in);
        if (ack != 0) {
          System.out.println("ACK was " + ack);
          return;
        }
      }

      // send "C0644 filesize filename", where filename should not include '/'
      long filesize = _lfile.length();
      command = "C0644 " + filesize + " ";
      if (lfile.lastIndexOf('/') > 0) {
        command += lfile.substring(lfile.lastIndexOf('/') + 1);
      } else {
        command += lfile;
      }
      command += "\n";
      out.write(command.getBytes());
      out.flush();
      ack = checkAck(in);
      if (ack != 0) {
        System.out.println("ACK was " + ack);
        return;
      }

      // send a content of lfile
      fis = new FileInputStream(lfile);
      byte[] buf = new byte[1024];
      while (true) {
        int len = fis.read(buf, 0, buf.length);
        if (len <= 0) {
          break;
        }
        out.write(buf, 0, len); //out.flush();
      }
      fis.close();
      fis = null;
      // send '\0'
      buf[0] = 0;
      out.write(buf, 0, 1);
      out.flush();
      ack = checkAck(in);
      if (ack != 0) {
        System.out.println("ACK was " + ack);
        return;
      }
      out.close();

      channel.disconnect();
      session.disconnect();

    } catch (Exception e) {
      GNS.getLogger().severe(e.toString());
      try {
        if (fis != null) {
          fis.close();
        }
      } catch (Exception ee) {
      }
    }
  }

  public static Session authenticateWithKey(JSch jsch, String user, String host, File keyFile) throws JSchException {
    if (keyFile == null) {
      JFileChooser chooser = new JFileChooser();
      chooser.setDialogTitle("Choose your privatekey(ex. ~/.ssh/id_dsa)");
      chooser.setFileHidingEnabled(false);
      int returnVal = chooser.showOpenDialog(null);
      if (returnVal == JFileChooser.APPROVE_OPTION) {
        //System.out.println("You chose " + chooser.getSelectedFile().getAbsolutePath());
        keyFile = chooser.getSelectedFile();
      }
    }

    if (keyFile == null) {
      return null;
    } else {
      jsch.addIdentity(keyFile.getAbsolutePath());
    }

    return authenticateWithPassword(jsch, user, host);

  }

  public static Session authenticateWithPassword(JSch jsch, String user, String host) throws JSchException {

    if (host == null || user == null) {
      String host_user = JOptionPane.showInputDialog("Enter username@hostname",
              (user == null ? System.getProperty("user.name") : user)
              + (host == null ? "@localhost" : host));

      user = host_user.substring(0, host_user.indexOf('@'));
      host = host_user.substring(host_user.indexOf('@') + 1);
    }

    Session session = jsch.getSession(user, host, 22);
    // Disable those pesky "The authenticity of host" message
    // another way to do this is to use a know-hosts file
    java.util.Properties props = new Properties();
    props.setProperty("StrictHostKeyChecking", "no");
    session.setConfig(props);
    return session;
  }

  static int checkAck(InputStream in) throws IOException {
    int b = in.read();
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //          -1
    if (b == 0) {
      return b;
    }
    if (b == -1) {
      return b;
    }

    if (b == 1 || b == 2) {
      StringBuffer sb = new StringBuffer();
      int c;
      do {
        c = in.read();
        sb.append((char) c);
      } while (c != '\n');
      if (b == 1) { // error
        GNS.getLogger().warning(sb.toString());
      }
      if (b == 2) { // fatal error
        GNS.getLogger().warning(sb.toString());
      }
    }
    return b;
  }

  public static boolean isVerbose() {
    return verbose;
  }

  public static void setVerbose(boolean verbose) {
    SSHClient.verbose = verbose;
  }
 
  public static void main(String[] arg) {
    String host = "23.21.160.80";
    String scriptPath = "/Users/westy/Documents/Code/GNRS-westy/scripts/5nodesregions/";
    String script = "installnosudo.sh";
    File keyFile = new File("/Users/westy/.ssh/aws.pem");

    scpTo("ec2-user", host, keyFile, scriptPath + script, script);
    SSHClient.execWithSudoNoPass("ec2-user", host, keyFile, "chmod ugo+x " + script);
    SSHClient.execWithSudoNoPass("ec2-user", host, keyFile, "./" + script);
  }
}
