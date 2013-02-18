//
// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//

package atnf.atoms.mon.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import atnf.atoms.mon.Alarm;
import atnf.atoms.mon.AlarmEvent;
import atnf.atoms.mon.AlarmEventListener;
import atnf.atoms.mon.PointDescription;
import atnf.atoms.mon.gui.AlarmPopupFrame;
import atnf.atoms.time.RelTime;

/**
 * Class which periodically polls the server for the status of priority alarms, and presents a high level interface to client
 * objects which need to interact with alarms.
 * 
 * @author David Brodrick
 * @author Kalinga Hulugalle
 */
public class AlarmMaintainer implements Runnable {
  /** The polling interval. */
  private static final RelTime theirPollInterval = RelTime.factory(10000000);

  /** Record of points which are currently in a priority alarm state. */
  private static HashMap<PointDescription, Alarm> theirAlarms = new HashMap<PointDescription, Alarm>(500, 1000);

  /** Boolean to determine whether to allow automatic alarm popups or not */
  public static boolean autoAlarms = false;

  static {
    // Start the polling thread
    new Thread(new AlarmMaintainer(), "AlarmMaintainer Poller").start();
  }

  /** List of AlarmEventListeners currently registered to this source **/
  private static ArrayList<AlarmEventListener> theirListeners = new ArrayList<AlarmEventListener>();

  /**
   * Registers the specified listener with the AlarmEvent source
   * 
   * @param listener
   *          - the listener to be registered
   */
  public static void addListener(AlarmEventListener listener) {
    theirListeners.add(listener);
  }

  /**
   * Removes the specified listener from the list of currently registered listeners
   * 
   * @param listener
   *          - the listener to be deregistered
   */
  public static void removeListener(AlarmEventListener listener) {
    theirListeners.remove(listener);
  }

  /**
   * For a given Alarm, creates a new AlarmEvent and distributes it to all the registered listeners
   * 
   * @param a
   *          The Alarm that the listeners are notified about
   */
  private synchronized static void fireAlarmEvent(Alarm a) {
    AlarmEvent ae = new AlarmEvent(a.getPointDesc(), a);
    for (AlarmEventListener ael : theirListeners) {
      ael.onAlarmEvent(ae);
    }

    if (a.isAlarming() && a.getPriority() == 3) {
      displayAlarmNotification(a);
    }

  }

  /**
   * Method to display automatically an Alarm notification popup frame if the alarm in question is actively alarming and is part of
   * the highest bracket of alarm priorities.
   * 
   * @param a
   *          The Alarm to display the notification about
   */
  private synchronized static void displayAlarmNotification(final Alarm a) {
    new Thread() {
      @Override
      public void run() {
        if (a.isAlarming() && a.getPriority() == 3) {
          AlarmPopupFrame apf = new AlarmPopupFrame(a);
          apf.pack();
          apf.setVisible(true);
        }
      }
    }.start();
  }

  @Override
  public void run() {
    while (true) {
      try {
        if (autoAlarms) { // if automatic alarm notifications are enabled
          // Fetch the list of all alarms from the server
          Vector<Alarm> newalarms = MonClientUtil.getServer().getAllAlarms();

          if (newalarms != null) {
            // Update the local alarm information
            synchronized (theirAlarms) {
              for (Alarm a : newalarms) {
                theirAlarms.put(a.getPointDesc(), a);
                displayAlarmNotification(a);
              }
            }

            // Notify any listeners about the updates
            for (Alarm a : newalarms) {
              fireAlarmEvent(a);
            }
          }
        } else if (theirListeners.size() > 0) { // if automatic alarms are not enabled,
          // only update when there are registered listeners
          // Fetch the list of all alarms from the server
          Vector<Alarm> newalarms = MonClientUtil.getServer().getAllAlarms();

          if (newalarms != null) {
            // Update the local alarm information
            synchronized (theirAlarms) {
              for (Alarm a : newalarms) {
                theirAlarms.put(a.getPointDesc(), a);
              }
            }

            // Notify any listeners about the updates
            for (Alarm a : newalarms) {
              fireAlarmEvent(a);
            }
          }
        }
      } catch (Exception e) {
      }

      // Sleep awhile
      try {
        theirPollInterval.sleep();
      } catch (Exception e) {
      }
    }
  }

  /** Write the specified alarm acknowledgement change to the server. */
  public static void setAcknowledged(String point, boolean ack, String username, String password) {
    Vector<String> pointname = new Vector<String>(1);
    pointname.add(point);
    setAcknowledged(pointname, ack, username, password);
  }

  /** Write the specified alarm acknowledgement change to the server, and also temporarily to the local list until it gets updated */
  public static void setAcknowledged(Vector<String> pointnames, boolean ack, String username, String password) {
    try {
      MonClientUtil.getServer().acknowledgeAlarms(pointnames, ack, username, password);
      for (String s : pointnames) {
        theirAlarms.get(PointDescription.getPoint(s)).setAcknowledged(ack);
      }
    } catch (Exception e) {
    }
  }

  /** Write the specified alarm shelving change to the server. */
  public static void setShelved(String point, boolean shelve, String username, String password) {
    Vector<String> pointname = new Vector<String>(1);
    pointname.add(point);
    setShelved(pointname, shelve, username, password);
  }

  /** Write the specified alarm shelving change to the server, and also temporarily to the local list until it gets updated */
  public static void setShelved(Vector<String> pointnames, boolean shelve, String username, String password) {
    try {
      MonClientUtil.getServer().shelveAlarms(pointnames, shelve, username, password);
      for (String s : pointnames) {
        theirAlarms.get(PointDescription.getPoint(s)).setShelved(shelve);
      }
    } catch (Exception e) {
    }
  }

  /** Get the list of priority alarms currently in an alarm state (acknowledged or not) or not in an alarm but shelved. */
  public static Vector<Alarm> getAlarms() {
    Vector<Alarm> res;
    synchronized (theirAlarms) {
      res = new Vector<Alarm>(theirAlarms.size());
      Iterator<Alarm> i = theirAlarms.values().iterator();
      while (i.hasNext()) {
        Alarm thisalarm = i.next();
        if (thisalarm.isAlarming() || thisalarm.isShelved()) {
          res.add(thisalarm);
        }
      }
    }
    return res;
  }

  /** Get the list of all alarms currently defined in the system. */
  public static Vector<Alarm> getAllAlarms() {
    Vector<Alarm> res;
    synchronized (theirAlarms) {
      res = new Vector<Alarm>(theirAlarms.size());
      Iterator<Alarm> i = theirAlarms.values().iterator();
      while (i.hasNext()) {
        Alarm thisalarm = i.next();
        res.add(thisalarm);
      }
    }
    return res;
  }

  /** Get the alarm for the given point, or null if not defined. */
  public static Alarm getAlarm(PointDescription point) {
    return theirAlarms.get(point);
  }

  /** Get the alarm for the given point, or null if not defined. */
  public static Alarm getAlarm(String pointname) {
    Alarm res = null;
    PointDescription point = PointDescription.getPoint(pointname);
    if (point != null) {
      res = getAlarm(point);
    }
    return res;
  }

  /**
   * Initialises a simple Alarm locally if it is not already defined.
   * 
   * @param point
   *          The PointDescription the new Alarm should be based on
   */
  public static void setAlarm(PointDescription point) {
    Alarm res = getAlarm(point);
    if (res == null) {
      res = new Alarm(point);
      theirAlarms.put(point, res);
    }
  }

  /** Simple test method to start the polling engine. */
  public static final void main(String[] args) {
    AlarmEventListener listener = new AlarmEventListener() {
      public void onAlarmEvent(AlarmEvent event) {
        System.err.println(event.getAlarm());
      }
    };
    AlarmMaintainer.addListener(listener);
    while (true) {
      try {
        theirPollInterval.sleep();
      } catch (Exception e) {
      }
    }
  }
}
