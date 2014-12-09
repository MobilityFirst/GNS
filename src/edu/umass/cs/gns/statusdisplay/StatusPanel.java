package edu.umass.cs.gns.statusdisplay;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.TreeSet;
import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 *
 * @author westy
 */
public class StatusPanel extends JPanel implements ActionListener {

  private JButton terminateButton;
  private StatusTextArea statusText;
  private boolean terminateButtonDisabled = false;

  /**
   * Creates new form StatusPanel
   */
  public StatusPanel() {
    initComponents();
  }

  /**
   * This method is called from within the constructor to initialize the form.
   */
  private void initComponents() {
    setLayout(new BorderLayout());
    updateComponents();
  }
  private static int ROWHEIGHT = 30;

  public void updateComponents() {
    //System.out.println("update");
    removeAll();
    Box box = Box.createVerticalBox();
    TreeSet<StatusEntry> entries = new TreeSet(StatusModel.getInstance().getEntries());
    // add the header
    StatusPane pane = new StatusPane(null);
    pane.setAlignmentX(Component.LEFT_ALIGNMENT);
    box.add(pane);
    for (StatusEntry entry : entries) {
      pane = new StatusPane(entry.getId());
      pane.setAlignmentX(Component.LEFT_ALIGNMENT);
      box.add(pane);
    }
    JPanel boxPanel = new JPanel();
    boxPanel.add(box);
    JScrollPane scrollPane = new JScrollPane(boxPanel);
    add(scrollPane, BorderLayout.WEST);
    terminateButton = new JButton("Terminate All");
    terminateButton.setToolTipText("Terminate");
    terminateButton.setActionCommand("Terminate");
    terminateButton.addActionListener(this);
    terminateButton.setEnabled(!terminateButtonDisabled);
//    terminateButton.setFocusPainted(false);
//    terminateButton.setBorderPainted(false);
    JPanel bottomPanel = new JPanel();
    bottomPanel.setLayout(new BorderLayout());
    bottomPanel.add(terminateButton, BorderLayout.SOUTH);
    statusText = new StatusTextArea();
    JScrollPane statusTextScroller = new JScrollPane(statusText);
    statusTextScroller.setPreferredSize(new Dimension(600, ROWHEIGHT * 5));
    bottomPanel.add(statusTextScroller, BorderLayout.NORTH);
    add(bottomPanel, BorderLayout.SOUTH);
    validate();
    //setPreferredSize(new Dimension(1000, StatusModel.getInstance().getEntries().size() * ROWHEIGHT + 50));
  }

  @Override
  public void actionPerformed(ActionEvent evt) {
    Object source = evt.getSource();
    String command = evt.getActionCommand();
    if ("Terminate".equals(command)) {
      terminateButtonDisabled = true; // for next update
      terminateButton.setEnabled(false);
      new Thread(
              new Runnable() {
                @Override
                public void run() {
                  //EC2Installer.terminateRunSet(EC2Installer.currentRunSetName());
                }
              }).start();

    }
  }
}
