package edu.umass.cs.amazontools.examples;

import edu.umass.cs.networktools.UserInfoPrompted;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;

/**
 *
 * @author westy
 */
/* -*-mode:java; c-basic-offset:2; indent-tabs-mode:nil -*- */
/**
 * This program will demonstrate the user authentification by public key. $ CLASSPATH=.:../build javac
 * UserAuthPubKey.java $ CLASSPATH=.:../build java UserAuthPubKey You will be asked username, hostname,
 * privatekey(id_dsa) and passphrase. If everything works fine, you will get the shell prompt
 *
 */
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
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
