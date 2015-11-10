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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.util;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;

/**
 * A modal dialog to let the user choose one string from a list.
 */
public class ChooserDialog extends JDialog implements ActionListener {

  private static ChooserDialog dialog;
  private static String returnValue = "";
  private final JList<Object> list;

  /**
   * Set up and show the dialog.
   *
   * @param frameComp frame that this dialog depends on, can be null
   * @param locationComp component on which this dialog should appear, can be null
   * @param labelText label for the window
   * @param title title for the selection list
   * @param possibleValues the array of String values to choose from
   * @param initialValue the default returnValue
   * @param longestValue the longest returnValue (used for dialog sizing), can be null
   * @return the returnValue selected
   */
  public static String showDialog(Component frameComp,
          Component locationComp,
          String labelText,
          String title,
          String[] possibleValues,
          String initialValue,
          String longestValue) {
    Frame frame = JOptionPane.getFrameForComponent(frameComp);
    dialog = new ChooserDialog(frame,
            locationComp,
            labelText,
            title,
            possibleValues,
            initialValue,
            longestValue);
    dialog.setVisible(true);
    return returnValue;
  }

  private void setReturnValue(String newValue) {
    returnValue = newValue;
    list.setSelectedValue(returnValue, true);
  }

  private ChooserDialog(Frame frame,
          Component locationComp,
          String labelText,
          String title,
          Object[] data,
          String initialValue,
          String longValue) {
    super(frame, title, true);

    //Create and initialize the buttons.
    JButton cancelButton = new JButton("Cancel");
    cancelButton.addActionListener(this);
    //
    final JButton setButton = new JButton("Choose");
    setButton.setActionCommand("Choose");
    setButton.addActionListener(this);
    getRootPane().setDefaultButton(setButton);

    //main part of the dialog
    list = new JList<>(data);

    list.setSelectionMode(ListSelectionModel.SINGLE_INTERVAL_SELECTION);
    if (longValue != null) {
      list.setPrototypeCellValue(longValue); //get extra space
    }
    list.setLayoutOrientation(JList.HORIZONTAL_WRAP);
    list.setVisibleRowCount(-1);
    list.addMouseListener(new MouseAdapter() {
      @Override
      public void mouseClicked(MouseEvent e) {
        if (e.getClickCount() == 2) {
          setButton.doClick(); //emulate button click
        }
      }
    });
    JScrollPane listScroller = new JScrollPane(list);
    listScroller.setPreferredSize(new Dimension(250, 80));
    listScroller.setAlignmentX(LEFT_ALIGNMENT);

        //Create a container so that we can add a title around
    //the scroll pane.  Can't add a title directly to the
    //scroll pane because its background would be white.
    //Lay out the label and scroll pane from top to bottom.
    JPanel listPane = new JPanel();
    listPane.setLayout(new BoxLayout(listPane, BoxLayout.PAGE_AXIS));
    JLabel label = new JLabel(labelText);
    label.setLabelFor(list);
    listPane.add(label);
    listPane.add(Box.createRigidArea(new Dimension(0, 5)));
    listPane.add(listScroller);
    listPane.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));

    //Lay out the buttons from left to right.
    JPanel buttonPane = new JPanel();
    buttonPane.setLayout(new BoxLayout(buttonPane, BoxLayout.LINE_AXIS));
    buttonPane.setBorder(BorderFactory.createEmptyBorder(0, 10, 10, 10));
    buttonPane.add(Box.createHorizontalGlue());
    buttonPane.add(cancelButton);
    buttonPane.add(Box.createRigidArea(new Dimension(10, 0)));
    buttonPane.add(setButton);

    //Put everything together, using the content pane's BorderLayout.
    Container contentPane = getContentPane();
    contentPane.add(listPane, BorderLayout.CENTER);
    contentPane.add(buttonPane, BorderLayout.PAGE_END);

    //Initialize values.
    setReturnValue(initialValue);
    pack();
    setLocationRelativeTo(locationComp);
  }

  //Handle clicks on the Choose and Cancel buttons.
  @Override
  public void actionPerformed(ActionEvent e) {
    if ("Choose".equals(e.getActionCommand())) {
      ChooserDialog.returnValue = (String) (list.getSelectedValue());
    }
    ChooserDialog.dialog.setVisible(false);
  }
}
