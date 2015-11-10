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
package edu.umass.cs.gnsserver.utils;

import java.awt.GraphicsConfiguration;
import java.awt.GraphicsDevice;
import java.awt.GraphicsEnvironment;
import java.awt.Rectangle;
import javax.swing.JFrame;

/**
 *
 * @author westy
 */
public class ScreenUtils {
    
  /**
   *
   * @param frame
   */
  public static void putOnWidestScreen(JFrame frame) {
    GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
    GraphicsDevice[] gs = ge.getScreenDevices();
    int widestGD = -1;
    int widestGC = -1;
    double widestSize = 0;
    for (int j = 0; j < gs.length; j++) {
      GraphicsDevice gd = gs[j];
      GraphicsConfiguration[] gcs = gd.getConfigurations();
      for (int i = 0; i < gcs.length; i++) {
        Rectangle gcBounds = gcs[i].getBounds();
        System.out.println(gcBounds);
        if (gcBounds.getWidth() > widestSize) {
          widestGD = j;
          widestGC = i;
          widestSize = gcBounds.getWidth();
        }
      }
    }
    if (widestSize > 0) {
      GraphicsDevice gd = gs[widestGD];
      GraphicsConfiguration[] gcs = gd.getConfigurations();
      GraphicsConfiguration gc = gcs[widestGC];
      frame.setBounds(gc.getBounds()); 
    } else {
      throw new RuntimeException("No Screens Found");
    }
  }

  /**
   *
   * @param screen
   * @param frame
   */
  public static void showOnScreen(int screen, JFrame frame) {
    GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
    GraphicsDevice[] gs = ge.getScreenDevices();
    if (screen > -1 && screen < gs.length) {
      gs[screen].setFullScreenWindow(frame);
    } else if (gs.length > 0) {
      gs[0].setFullScreenWindow(frame);
    } else {
      throw new RuntimeException("No Screens Found");
    }
  }

  /**
   * For testing.
   */
  public static void showScreenDevices() {
    GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
    GraphicsDevice[] gs = ge.getScreenDevices();
    for (int j = 0; j < gs.length; j++) {
      GraphicsDevice gd = gs[j];
      GraphicsConfiguration[] gc = gd.getConfigurations();
      for (int i = 0; i < gc.length; i++) {
        Rectangle gcBounds = gc[i].getBounds();
        //System.out.println(gcBounds);
      }
    }
  }
  
}
