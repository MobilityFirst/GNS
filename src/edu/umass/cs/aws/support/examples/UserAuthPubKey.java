
package edu.umass.cs.aws.support.examples;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import edu.umass.cs.aws.networktools.UserInfoPrompted;

import javax.swing.*;
import java.awt.*;


public class UserAuthPubKey {


  public static void main(String[] arg) {

    try {
      JSch jsch = new JSch();

      JFileChooser chooser = new JFileChooser();
      chooser.setDialogTitle("Choose your privatekey(ex. ~/.ssh/id_dsa)");
      chooser.setFileHidingEnabled(false);
      int returnVal = chooser.showOpenDialog(null);
      if (returnVal == JFileChooser.APPROVE_OPTION) {
        System.out.println("You chose "
                + chooser.getSelectedFile().getAbsolutePath() + ".");
        jsch.addIdentity(chooser.getSelectedFile().getAbsolutePath() //			 , "passphrase"
        );
      }

      String host = null;
      if (arg.length > 0) {
        host = arg[0];
      } else {
        host = JOptionPane.showInputDialog("Enter username@hostname",
                System.getProperty("user.name")
                + "@localhost");
      }
      String user = host.substring(0, host.indexOf('@'));
      host = host.substring(host.indexOf('@') + 1);

      Session session = jsch.getSession(user, host, 22);

      // username and passphrase will be given via UserInfo interface.
      UserInfo ui = new UserInfoPrompted();
      session.setUserInfo(ui);
      session.connect();

      Channel channel = session.openChannel("shell");

      channel.setInputStream(System.in);
      channel.setOutputStream(System.out);

      channel.connect();
    } catch (HeadlessException | JSchException e) {
      System.out.println(e);
    }
  }
}
