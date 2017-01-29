
package edu.umass.cs.aws.support.examples;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import edu.umass.cs.aws.networktools.SSHClient;
import edu.umass.cs.aws.networktools.UserInfoPrompted;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


public class Sudo {


  public static void main(String[] arg) {
    try {
      JSch jsch = new JSch();

      Session session = SSHClient.authenticateWithKey(jsch, null, null, null);

      UserInfo ui = new UserInfoPrompted();
      session.setUserInfo(ui);
      session.connect();

      String command = JOptionPane.showInputDialog("Enter command, execed with sudo",
              "printenv SUDO_USER");

      String sudo_pass = null;
      {
        JTextField passwordField = new JPasswordField(8);
        Object[] ob = {passwordField};
        int result
                = JOptionPane.showConfirmDialog(null,
                        ob,
                        "Enter password for sudo",
                        JOptionPane.OK_CANCEL_OPTION);
        if (result != JOptionPane.OK_OPTION) {
          System.exit(-1);
        }
        sudo_pass = passwordField.getText();
      }

      Channel channel = session.openChannel("exec");

      // man sudo
      //   -S  The -S (stdin) option causes sudo to read the password from the
      //       standard input instead of the terminal device.
      //   -p  The -p (prompt) option allows you to override the default
      //       password prompt and use a custom one.
      ((ChannelExec) channel).setCommand("sudo -S -p '' " + command);

      InputStream in = channel.getInputStream();
      OutputStream out = channel.getOutputStream();
      ((ChannelExec) channel).setErrStream(System.err);
      ((ChannelExec) channel).setPty(true);
      channel.connect();

      out.write((sudo_pass + "\n").getBytes());
      out.flush();

      byte[] tmp = new byte[1024];
      while (true) {
        while (in.available() > 0) {
          int i = in.read(tmp, 0, 1024);
          if (i < 0) {
            break;
          }
          System.out.print(new String(tmp, 0, i));
        }
        if (channel.isClosed()) {
          System.out.println("exit-status: " + channel.getExitStatus());
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (Exception ee) {
        }
      }
      channel.disconnect();
      session.disconnect();
    } catch (JSchException | HeadlessException | IOException e) {
      System.out.println(e);
    }
  }
}
