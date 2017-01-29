
package edu.umass.cs.gnsclient.client;

import edu.umass.cs.utils.Config;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;


public class GNSClientConfig {
  
  private final static Logger LOGGER = Logger.getLogger(GNSClientConfig.class
          .getName());


  public static final Logger getLogger() {
    return LOGGER;
  }


  public static enum GNSCC implements Config.ConfigurableEnum {

    USE_GLOBAL_MONITOR(false),

    ENABLE_SECRET_KEY(true),

    //@Deprecated
    //VERIFICATION_SECRET("EXPOSED_SECRET"),

    BYTE_MODE(0),

    USE_JAVA_PREFERENCE(false),

    LOCAL_NAME_SERVER_PORT(24398);
    
    final Object defaultValue;
    
    GNSCC(Object defaultValue) {
      this.defaultValue = defaultValue;
    }


    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }


    @Override
    public String getConfigFileKey() {
      return "gigapaxosConfig";
    }


    @Override
    public String getDefaultConfigFile() {
      return "gns.client.properties";
    }
  }


  public static String readBuildVersion() {
    String result = "Unknown";
    try {
      Class<?> clazz = GNSClientConfig.class;
      //String className = clazz.getSimpleName() + ".class";
      //String classPath = clazz.getResource(className).toString();
      //System.out.println("readBuildVersion: classPath is " + classPath);
      // Look in all the resources that have META-INF to try to find the
      // gnsclient jar file.
      Enumeration<URL> urls = clazz.getClassLoader().getResources("META-INF");
      while (urls.hasMoreElements()) {
        URL url = urls.nextElement();
        //System.out.println("url: " + url.toString());
        String classPath = url.toString();
        if (classPath.startsWith("jar") && classPath.contains("gnsclient-")) {
          String manifestPath = classPath.substring(0,
                  classPath.lastIndexOf("!") + 1)
                  + "/META-INF/MANIFEST.MF";
          //System.out.println("readBuildVersion: manifestPath is " + manifestPath);
          Manifest manifest = new Manifest(new URL(manifestPath).openStream());
          Attributes attr = manifest.getMainAttributes();
          return attr.getValue("Build-Version");
        }
      }
    } catch (IOException e) {
    }
    return result;
  }
}
