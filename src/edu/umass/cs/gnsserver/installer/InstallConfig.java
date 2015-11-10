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
package edu.umass.cs.gnsserver.installer;

import edu.umass.cs.gnsserver.main.GNS;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Parses a properties file to get all info needed to install the GNS on hosts.
 * 
 * @author westy
 */
public class InstallConfig {

  private static final String USER_NAME = "userName";
  private static final String KEY_FILE = "keyFile";
  private static final String HOST_TYPE = "hostType";
  private static final String DATA_STORE_NAME = "dataStoreName";
  private static final String INSTALL_PATH = "installPath";
  private static final String JAVA_COMMAND = "javaCommand";

  private String username;
  private String keyFile;
  private String hostType;
  private DataStoreType dataStoreType;
  private String installPath;
  private String javaCommand;

  /**
   * Returns the username.
   * 
   * @return the username
   */
  public String getUsername() {
    return username;
  }

  /**
   * Returns the key file.
   * 
   * @return the key file
   */
  public String getKeyFile() {
    return keyFile;
  }

  /**
   * Returns the host type.
   * 
   * @return the host type
   */
  public String getHostType() {
    return hostType;
  }

  /**
   * Returns the dataStoreType.
   * 
   * @return the dataStoreType
   */
  public DataStoreType getDataStoreType() {
    return dataStoreType;
  }

  /**
   * Returns the install path.
   * 
   * @return the install path
   */
  public String getInstallPath() {
    return installPath;
  }

  /**
   * Returns the java command.
   * 
   * @return the java command
   */
  public String getJavaCommand() {
    return javaCommand;
  }

  /**
   * Creates an instance of InstallConfig.
   * 
   * @param filename
   */
  public InstallConfig(String filename) {
    try {
      loadPropertiesFile(filename);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem loading installer config file: " + e);
    }
  }

  private void loadPropertiesFile(String filename) throws IOException {
    Properties properties = new Properties();

    File f = new File(filename);
    if (f.exists() == false) {
      throw new FileNotFoundException("Install config file not found:" + filename);
    }
    InputStream input = new FileInputStream(filename);
    properties.load(input);
    // username=ec2-user
    // keyfile=aws.pem
    // hosttype=linux
    // datastorename=MONGO
    //#installpath=/home/westy
    this.username = properties.getProperty(USER_NAME, System.getProperty("user.name"));
    this.keyFile = properties.getProperty(KEY_FILE);
    this.hostType = properties.getProperty(HOST_TYPE, "linux");
    this.dataStoreType = DataStoreType.valueOf(properties.getProperty(DATA_STORE_NAME, GNSInstaller.DEFAULT_DATA_STORE_TYPE.name()).toUpperCase());
    this.installPath = properties.getProperty(INSTALL_PATH);
    this.javaCommand = properties.getProperty(JAVA_COMMAND);
  }

  @Override
  public String toString() {
    return "InstallConfig{" + "username=" + username + ", keyFile=" + keyFile + ", hostType=" + hostType + ", dataStoreType=" + dataStoreType + ", installPath=" + installPath + ", javaCommand=" + javaCommand + '}';
  }

  /**
   * The main routine. For testing only.
   * 
   * @param args
   */
  public static void main(String[] args) {
    String filename = GNS.WESTY_GNS_DIR_PATH + "/conf/ec2_small/installer_config";
    InstallConfig config = new InstallConfig(filename);
    System.out.println(config.toString());
  }
}
