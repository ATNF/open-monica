// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon;

import org.apache.log4j.Logger;

import java.util.*;
import java.io.*;

import atnf.atoms.mon.externalsystem.*;
import atnf.atoms.mon.util.*;
import atnf.atoms.mon.archiver.*;
import atnf.atoms.mon.comms.*;

import com.zeroc.Ice.ObjectAdapter;

/**
 * Top-level class with methods for starting and stopping MoniCA.
 *
 * @author David Brodrick
 */
public class MoniCAMain {
  /** Logger. */
  private static Logger theirLogger = Logger.getLogger(MoniCAMain.class.getName());

  /** Records if the server is fully up and running. */
  private static boolean theirServerRunning = false;

  /**
   * The ICE Adapter to be used for initialising the ICE server interface. This is optional, if left as null then a new adapter will
   * be created using configuration parameters.
   */
  private static ObjectAdapter theirICEAdapter = null;

  /** Specify the ICE Communicator to be used by the ICE server interface. */
  public static void setICEAdapter(ObjectAdapter a) {
    theirICEAdapter = a;
  }

  /**
   * Stop MoniCA and free resources.
   *
   * @return True if successfully stopped, False if a problem was encountered.
   */
  public static boolean stop() {
    theirLogger.info("Stopping MoniCA Server..");
    System.exit(0);
    return true;
  }

  /**
   * Create all of the ExternalSystems from whichever configuration resources are available.
   *
   * @return True if successful, False if there was a fatal error.
   */
  private static boolean createExternalSystems() {
    boolean foundsystems = false;
    // The conf.d subdirectory name for where the external system definitions are found
    final String SYSSUBDIR = "/systems.d/";
    // The fixed name of the point definitions resource
    final String SYSRESNAME = "monitor-sources.txt";

    // Load the configuration built into the jar/classpath file, if found
    InputStream exsystemsfile = MoniCAMain.class.getClassLoader().getResourceAsStream(SYSRESNAME);
    if (exsystemsfile != null) {
      theirLogger.info("Loading external systems definitions from \"" + SYSRESNAME + "\" resource");
      ExternalSystem.init(new InputStreamReader(exsystemsfile));
      foundsystems = true;
    }

    // Check for definition files in the configuration directory
    String confdir = System.getProperty("MoniCA.ConfDir");
    if (confdir == null) {
      confdir = MonitorConfig.getProperty("ConfDir");
    }
    if (confdir != null) {
      confdir = confdir + SYSSUBDIR;
      File confdirf = new File(confdir);
      if (confdirf.exists() && confdirf.isDirectory()) {
        // Directory looks valid, list the contents
        File[] flist = confdirf.listFiles();
        if (flist != null) {
          // Sort the file list lexicographically to enabled expected conf.d behaviour
          Arrays.sort(flist);
          for (File f : flist) {
	      if (!(f.isFile() && f.getName().endsWith(".txt"))) {
              theirLogger.warn("External systems definition file \"" + f + "\" is not a file");
            } else {
              // Looks like it should be a valid points definition file, so parse it
              theirLogger.info("Loading external systems definitions from \"" + f + "\"");
              FileReader fr = null;
              try {
                fr = new FileReader(f);
                ExternalSystem.init(fr);
                foundsystems = true;
              } catch (Exception e) {
                theirLogger.error("While parsing file: " + e);
              } finally {
                if (fr != null) {
                  try {
                    fr.close();
                  } catch (IOException e) {
                  }
                }
              }
            }
          }
        }
      }
    }

    if (!foundsystems) {
      // Might be intentional, so log, but as info not error
      theirLogger.info("No external system definitions were found");
    }

    return true;
  }

  /**
   * Create all of the PointDescriptions from whichever configuration resources are available.
   *
   * @return True if successful, False if there was a fatal error.
   */
  private static boolean createPoints() {
    boolean foundpoints = false;
    // The conf.d subdirectory name for where point definitions are found
    final String POINTSSUBDIR = "/points.d/";
    // The fixed name of the point definitions resource
    final String POINTSRESNAME = "monitor-points.txt";

    // Load the configuration built into the jar/classpath file, if found
    InputStream pointsfile = MoniCAMain.class.getClassLoader().getResourceAsStream(POINTSRESNAME);
    if (pointsfile != null) {
      InputStreamReader isr = null;
      try {
        theirLogger.info("Loading point definitions from \"" + POINTSRESNAME + "\" resource");
        isr = new InputStreamReader(pointsfile);
        PointDescription.parseFile(isr);
        foundpoints = true;
      } catch (Exception e) {
        theirLogger.error("While parsing point definitions: " + e);
      } finally {
        if (isr != null) {
          try {
            isr.close();
          } catch (Exception f) {
          }
        }
      }
    }

    // Check for definition files in the configuration directory
    String confdir = System.getProperty("MoniCA.ConfDir");
    if (confdir == null) {
      confdir = MonitorConfig.getProperty("ConfDir");
    }
    if (confdir != null) {
      confdir = confdir + POINTSSUBDIR;
      File confdirf = new File(confdir);
      if (confdirf.exists() && confdirf.isDirectory()) {
        // Directory looks valid, list the contents
        File[] flist = confdirf.listFiles();
        if (flist != null) {
          // Sort the file list lexicographically to enabled expected conf.d behaviour
          Arrays.sort(flist);
          for (File f : flist) {
	      if (!(f.isFile() && f.getName().endsWith(".txt"))) {
              theirLogger.warn("Points definition file \"" + f + "\" is not a file or does not end in '.txt'");
            } else {
              // Looks like it should be a valid points definition file, so parse it
              theirLogger.info("Loading point definitions from \"" + f + "\"");
              FileReader fr = null;
              try {
                fr = new FileReader(f);
                PointDescription.parseFile(fr);
                foundpoints = true;
              } catch (Exception e) {
                theirLogger.error("While parsing point definitions: " + e);
              } finally {
                if (fr != null) {
                  try {
                    fr.close();
                  } catch (IOException e) {
                  }
                }
              }
            }
          }
        }
      }
    }

    //Create all of the server-side fields for the points
    PointDescription[] allpoints = PointDescription.getAllUniquePoints();
    for (PointDescription point : allpoints) {
      try {
        point.populateServerFields();
      } catch (Exception e) {
        theirLogger.error("While creating point \"" + point.getFullName() + "\": "+ e);
      }
    }

    // Points have all been created now
    PointDescription.setPointsCreated();

    if (!foundpoints) {
      // Might be intentional, so log, but as info not error
      theirLogger.info("No point definitions were found");
    }

    return true;
  }

  /**
   * Create all of the SavedSetups from whichever configuration resources are available.
   *
   * @return True if successful, False if there was a fatal error.
   */
  private static boolean createSetups() {
    boolean foundsetups = false;
    // The conf.d subdirectory name for where point definitions are found
    final String SETUPSUBDIR = "/setups.d/";
    // The fixed name of the point definitions resource
    final String SETUPRESNAME = "monitor-setups.txt";

    InputStream setupfile = MoniCAMain.class.getClassLoader().getResourceAsStream(SETUPRESNAME);
    if (setupfile != null) {
      InputStreamReader isr = null;
      try {
        isr = new InputStreamReader(setupfile);
        Vector<SavedSetup> setups = SavedSetup.parseFile(isr);
        theirLogger.debug("Recovered " + setups.size() + " SavedSetups from resource \"" + SETUPRESNAME + "\"");
        for (int i = 0; i < setups.size(); i++) {
          SavedSetup.addSetup((SavedSetup) setups.get(i));
        }
        foundsetups = true;
      } catch (Exception e) {
        theirLogger.fatal("Can't parse saved setup file: " + SETUPRESNAME);
        return false;
      } finally {
        if (isr != null) {
          try {
            isr.close();
          } catch (Exception f) {
          }
        }
      }
    }

    // Check for definition files in the configuration directory
    String confdir = System.getProperty("MoniCA.ConfDir");
    if (confdir == null) {
      confdir = MonitorConfig.getProperty("ConfDir");
    }
    if (confdir != null) {
      confdir = confdir + SETUPSUBDIR;
      File confdirf = new File(confdir);
      if (confdirf.exists() && confdirf.isDirectory()) {
        // Directory looks valid, list the contents
        File[] flist = confdirf.listFiles();
        if (flist != null) {
          // Sort the file list lexicographically to enabled expected conf.d behaviour
          Arrays.sort(flist);
          for (File f : flist) {
	      if (!(f.isFile() && f.getName().endsWith(".txt"))) {
              theirLogger.warn("Saved setups definition file \"" + f + "\" is not a file");
            } else {
              // Looks like it should be a valid points definition file, so parse it
              theirLogger.info("Loading al systems definitions from \"" + f + "\"");
              FileReader fr = null;
              try {
                fr = new FileReader(f);
                Vector<SavedSetup> setups = SavedSetup.parseFile(fr);
                theirLogger.debug("Recovered " + setups.size() + " SavedSetups from file \"" + f + "\"");
                for (int i = 0; i < setups.size(); i++) {
                  SavedSetup.addSetup((SavedSetup) setups.get(i));
                }
                foundsetups = true;
              } catch (Exception e) {
                theirLogger.error("While parsing file: " + e);
              } finally {
                if (fr != null) {
                  try {
                    fr.close();
                  } catch (IOException e) {
                  }
                }
              }
            }
          }
        }
      }
    }

    if (!foundsetups) {
      // Might be intentional, so log, but as info not error
      theirLogger.info("No saved setup definitions were found");
    }

    return true;
  }

  /**
   * Start MoniCA.
   *
   * @return True if successfully stopped, False if a problem was encountered.
   */
  public static boolean start() {
    theirLogger.info("Starting MoniCA Server..");

    // Create the archiver to store historical data
    theirLogger.debug("Creating PointArchiver");
    PointArchiver pa = null;
    try {
      Class archiverClass = Class.forName("atnf.atoms.mon.archiver.PointArchiver" + MonitorConfig.getProperty("Archiver"));
      pa = (PointArchiver) (archiverClass.newInstance());
      PointArchiver.setPointArchiver(pa);
    } catch (Exception e) {
      theirLogger.fatal("While creating PointArchiver:" + e);
      return false;
    }
    // Start archive thread
    ((Thread) pa).start();
    theirLogger.debug("PointArchiver created");

    String confdir = MonitorConfig.getProperty("ConfDir");
    if (confdir != null) {
      File confdirf = new File(confdir);
      if (!confdirf.exists()) {
        theirLogger.warn("Configuration directory \"" + confdir + "\" was specified, but doesn't exist");
        confdir = null;
      } else if (!confdirf.isDirectory()) {
        theirLogger.warn("Configuration directory \"" + confdir + "\" was specified, but is not a directory!");
        confdir = null;
      }
    }

    // Initialise all the ExternalSystems
    theirLogger.debug("Creating ExternalSystems");
    if (!createExternalSystems()) {
      return false;
    }
    theirLogger.debug("ExternalSystems created");

    // Create all the points
    theirLogger.debug("Creating PointDescriptions");
    if (!createPoints()) {
      return false;
    }
    theirLogger.debug("PointDescriptions created");

    // Recover all the SavedSetups
    theirLogger.debug("Creating SavedSetups");
    if (!createSetups()) {
      return false;
    }
    theirLogger.debug("SavedSetups created");

    // If no RADIUS server is defined then approve all auth requests (for backwards compatibility)
    RADIUSAuthenticator.setDefaultAuthMode(true);

    // Set the encryption keys
    KeyKeeper.getExponent();

    // Add shutdown listener to flush archive
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        ExternalSystem.stopAll();
        PointArchiver.getPointArchiver().flushArchive();
      }
    });

    // Start the data collection
    theirLogger.debug("Starting ExternalSystems");
    ExternalSystem.startAll();
    theirLogger.debug("ExternalSystems started");

    // Server is now running
    theirServerRunning = true;

    return true;
  }

  /**
   * Open the network server interfaces.
   */
  public static void openInterfaces() {
    new MoniCAServerASCII();
    if (theirICEAdapter == null) {
      MoniCAIceI.startIceServer();
    } else {
      MoniCAIceI.startIceServer(theirICEAdapter);
    }
    if (Boolean.parseBoolean(MonitorConfig.getProperty("PubSubEnabled", "false"))) {
      theirLogger.debug("Starting pub/sub server");
      new PubSubManager();
    }
  }

  /**
   * Close the network server interfaces.
   */
  public static void closeInterfaces() {
    MoniCAServerASCII.stopAll();
    MoniCAIceI.stopIceServer();
    // TODO: Close pub/sub server
  }

  /** Report if the server is now fully started and up and running. */
  public static boolean serverFullyStarted() {
    return theirServerRunning;
  }

  public static void main(String[] argv) {
    // java.util.logging.Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME).addHandler(new JuliToLog4JHandler());
    // java.util.logging.Logger.global.addHandler(new JuliToLog4JHandler());

    // Start the system
    if (MoniCAMain.start()) {
      // Open the network server interfaces
      MoniCAMain.openInterfaces();
      // We're now underway!
      theirLogger.info("MoniCA System is GO!");
    } else {
      theirLogger.fatal("Failed to Start MoniCA System!");
    }
  }
}
