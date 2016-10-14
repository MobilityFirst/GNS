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

import com.jcraft.jsch.UIKeyboardInteractive;
import com.jcraft.jsch.UserInfo;
import java.awt.Container;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

/**
 * Gets user info.
 * 
 * @author westy
 */
public class UserInfoPrompted implements UserInfo, UIKeyboardInteractive {

  /**
   *
   * @return the password
   */
  @Override
  public String getPassword() {
    return passwd;
  }

  /**
   *
   * @param str
   * @return true if the user says yes
   */
  @Override
  public boolean promptYesNo(String str) {
    Object[] options = {"yes", "no"};
    int foo = JOptionPane.showOptionDialog(null,
            str,
            "Warning",
            JOptionPane.DEFAULT_OPTION,
            JOptionPane.WARNING_MESSAGE,
            null, options, options[0]);
    return foo == 0;
  }
  private String passwd;
  private final JTextField passwordField = new JPasswordField(20);

  /**
   *
   * @return the pass phrase
   */
  @Override
  public String getPassphrase() {
    return null;
  }

  /**
   *
   * @param message
   * @return the user response
   */
  @Override
  public boolean promptPassphrase(String message) {
    return true;
  }

  /**
   *
   * @param message
   * @return true if the user confirms
   */
  @Override
  public boolean promptPassword(String message) {
    Object[] ob = {passwordField};
    int result
            = JOptionPane.showConfirmDialog(null, ob, message,
                    JOptionPane.OK_CANCEL_OPTION);
    if (result == JOptionPane.OK_OPTION) {
      passwd = passwordField.getText();
      return true;
    } else {
      return false;
    }
  }

  /**
   *
   * @param message
   */
  @Override
  public void showMessage(String message) {
    JOptionPane.showMessageDialog(null, message);
  }
  final GridBagConstraints gbc
          = new GridBagConstraints(0, 0, 1, 1, 1, 1,
                  GridBagConstraints.NORTHWEST,
                  GridBagConstraints.NONE,
                  new Insets(0, 0, 0, 0), 0, 0);
  private Container panel;

  /**
   *
   * @param destination
   * @param name
   * @param instruction
   * @param prompt
   * @param echo
   * @return the array of strings
   */
  @Override
  public String[] promptKeyboardInteractive(String destination,
          String name,
          String instruction,
          String[] prompt,
          boolean[] echo) {
    panel = new JPanel();
    panel.setLayout(new GridBagLayout());

    gbc.weightx = 1.0;
    gbc.gridwidth = GridBagConstraints.REMAINDER;
    gbc.gridx = 0;
    panel.add(new JLabel(instruction), gbc);
    gbc.gridy++;

    gbc.gridwidth = GridBagConstraints.RELATIVE;

    JTextField[] texts = new JTextField[prompt.length];
    for (int i = 0; i < prompt.length; i++) {
      gbc.fill = GridBagConstraints.NONE;
      gbc.gridx = 0;
      gbc.weightx = 1;
      panel.add(new JLabel(prompt[i]), gbc);

      gbc.gridx = 1;
      gbc.fill = GridBagConstraints.HORIZONTAL;
      gbc.weighty = 1;
      if (echo[i]) {
        texts[i] = new JTextField(20);
      } else {
        texts[i] = new JPasswordField(20);
      }
      panel.add(texts[i], gbc);
      gbc.gridy++;
    }

    if (JOptionPane.showConfirmDialog(null, panel,
            destination + ": " + name,
            JOptionPane.OK_CANCEL_OPTION,
            JOptionPane.QUESTION_MESSAGE)
            == JOptionPane.OK_OPTION) {
      String[] response = new String[prompt.length];
      for (int i = 0; i < prompt.length; i++) {
        response[i] = texts[i].getText();
      }
      return response;
    } else {
      return null;  // cancel
    }
  }
}
