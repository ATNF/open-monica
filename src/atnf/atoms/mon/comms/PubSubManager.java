/*
 * Copyright (c) 2011 CSIRO Australia Telescope National Facility (ATNF) Commonwealth
 * Scientific and Industrial Research Organisation (CSIRO) PO Box 76, Epping NSW 1710,
 * Australia atnf-enquiries@csiro.au
 *
 * This file is part of the ASKAP software distribution.
 *
 * The ASKAP software distribution is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this
 * program; if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite
 * 330, Boston, MA 02111-1307 USA
 */

package atnf.atoms.mon.comms;

import com.zeroc.IceStorm.*;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.ObjectPrx;
import com.zeroc.Ice.InitializationData;
import com.zeroc.Ice.ObjectAdapter;

import org.apache.log4j.Logger;

import java.util.*;
import atnf.atoms.time.*;
import atnf.atoms.mon.*;
import atnf.atoms.mon.util.MonitorConfig;

/**
 * This class creates an IceStorm topic and listens for requests from clients
 * who wish to obtain publish/subscribe monitor updates.
 *
 * <P>
 * The process is basically as follows:
 * <ol>
 * <li>The client creates a topic on which it will receive updates.
 * <li>The client sends a message to the server's subscription topic, specifying
 * the list of points of interest and the name of the topic on which the server
 * should publish updates.
 * <li>MoniCA will publish the latest cached values onto the topic.
 * <li>The client must periodically send a keep alive. This involves sending the
 * name of the topic to which the data is being published, to the subscription
 * topic.
 * </ol>
 *
 * If the client doesn't send the keep-alive in the specified time then the
 * server will assume the client is dead and will destroy the topic, to prevent
 * buildup of stale topics in the IceStorm server. However for good practice the
 * client should also destroy the topic if it is being shutdown cleanly.
 *
 *
 *
 * @author David Brodrick
 */
public class PubSubManager {
  /**
   * Class encapsulating a client who is subscribed to updates from a set of
   * points.
   */
  public class PubSubClientInfo implements PointListener {
    /** The name of the topic on which to publish the client's data. */
    public String itsTopicName;

    /** The topic on which to publish the client's data. */
    public TopicPrx itsTopic;

    /** Proxy to the client. */
    protected PubSubClientPrx itsClient;

    /** Names of the points to which the client is subscribed. */
    public String[] itsPointNames;

    /** Time the client last sent a keep-alive signal. */
    public AbsTime itsLastKeepAliveTime = new AbsTime();

    /** Constructor. */
    public PubSubClientInfo(String topic, String[] points) throws Exception {
      itsTopicName = topic;
      itsPointNames = points;

      // Ensure all points exist before subscribing
      for (int i = 0; i < itsPointNames.length; i++) {
        if (!PointDescription.checkPointName(itsPointNames[i])) {
          throw new Exception("Specified point (" + itsPointNames[i] + ") does not exist");
        }
      }

      // Connect to the topic created by the client
      TopicManagerPrx topicManager;
      ObjectPrx obj = itsCommunicator.stringToProxy("IceStorm/TopicManager@IceStorm.TopicManager");
      topicManager = TopicManagerPrx.checkedCast(obj);
      itsTopic = topicManager.retrieve(itsTopicName);
      ObjectPrx pub = itsTopic.getPublisher().ice_twoway();
      itsClient = PubSubClientPrx.uncheckedCast(pub);

      // Get the last recorded data for each point
      PointData[] lastdata = new PointData[itsPointNames.length];
      for (int i = 0; i < itsPointNames.length; i++) {
        lastdata[i] = PointBuffer.getPointData(itsPointNames[i]);
        // Can't handle null's in Ice so map points with no values to null values
        if (lastdata[i]==null) {
          lastdata[i] = new PointData(itsPointNames[i]);
        }
      }

      // Publish the last data values
      PointDataIce[] lastdataice = MoniCAIceUtil.getPointDataAsIce(lastdata);

      itsClient.updateData(lastdataice);

      // Subscribe to updates from each point
      for (int i = 0; i < itsPointNames.length; i++) {
        PointDescription.getPoint(itsPointNames[i]).addPointListener(this);
      }
    }

    /** Callback for when a listened-to point updates. */
    public void onPointEvent(Object source, final PointEvent evt) {
      // Use an anonymous thread to publish the data
      new Thread("PubSub Delivery Thread") {
        public void run() {
          try {
            PointData[] pd = new PointData[] { evt.getPointData() };
            PointDataIce[] pdice = MoniCAIceUtil.getPointDataAsIce(pd);
            itsClient.updateData(pdice);
          } catch (Exception e) {
            itsLogger.error("Error publishing data to topic " + itsTopicName + ": " + e);
            // This client is now broken
            PubSubClientInfo.this.destroy();
          }
        }
      }.start();
    }

    /** Release all resources used by this client. */
    public void destroy() {
      // Destroy the topic
      if (itsTopic != null) {
        try {
          itsTopic.destroy();
        } catch (Exception e) {
        }
      }
      itsTopic = null;

      // Unsubscribe from points
      for (int i = 0; i < itsPointNames.length; i++) {
        PointDescription.getPoint(itsPointNames[i]).removePointListener(this);
      }
    }

    /** Reset the keep-alive timer. */
    public void resetKeepAlive() {
      itsLastKeepAliveTime = new AbsTime();
    }

    /** Get the time that the keep-alive was last reset. */
    public AbsTime getLastKeepAliveTime() {
      return itsLastKeepAliveTime;
    }
  };

  /**
   * Class implementing the subscriber to the control topic.
   */
  public class PubSubControlI implements PubSubControl {
    /** Handle a new subscription request. */
    public void subscribe(PubSubRequest req, Current curr) {
      itsLogger.debug("Received new subscription request for topic " + req.topicname + " with " + req.pointnames.length + " points");
      try {
        PubSubClientInfo newclient = new PubSubClientInfo(req.topicname, req.pointnames);
        synchronized (itsClients) {
          itsClients.put(req.topicname, newclient);
        }
      } catch (Exception e) {
        itsLogger.error("While establishing subscriptions for topic " + req.topicname + ": " + e);
      }
    }

    /** Cancel activity on the specified topic */
    public void unsubscribe(String topicname, Current curr) {
      synchronized (itsClients) {
        PubSubClientInfo client = itsClients.get(topicname);
        if (client != null) {
          itsLogger.debug("Cancelling activity on topic " + topicname);
          client.destroy();
          itsClients.remove(topicname);
        } else {
          itsLogger.debug("Received unsubscription request for non-existent topic " + topicname);
        }
      }
    }

    /** Record that the specified topic is still active. */
    public void keepalive(String topicname, Current curr) {
      //itsLogger.debug("Received keep-alive for topic " + topicname);
      synchronized (itsClients) {
        PubSubClientInfo thisclient = itsClients.get(topicname);
        if (thisclient != null) {
          thisclient.resetKeepAlive();
        } else {
          itsLogger.debug("Received keep-alive for abandoned topic " + topicname);
        }
      }
    }
  };

  /** Thread which purges abandoned clients. */
  public class AbandonedClientPurger extends Thread {
    public AbandonedClientPurger() {
      super("PubSub AbandonedClientPurger");
    }

    public void run() {
      while (true) {
        synchronized (itsClients) {
          // See if any clients have timed out
          AbsTime now = new AbsTime();
          Object[] keys = itsClients.keySet().toArray();
          for (int i = 0; i < itsClients.size(); i++) {
            String topicname = (String) keys[i];
            PubSubClientInfo client = itsClients.get(topicname);
            if (Time.diff(now, client.getLastKeepAliveTime()).getAsSeconds() > itsMaxKeepAliveTime.getAsSeconds()) {
              itsLogger.debug("Destroying data structures for abandoned topic " + topicname);
              client.destroy();
              itsClients.remove(topicname);
            }
          }
        }

        // Sleep until next time to check
        try {
          itsMaxKeepAliveTime.sleep();
        } catch (Exception e) {
        }
      }
    }
  };

  /** TCP port number for IceGrid locator service. */
  protected int itsPort;

  /** Host name for IceGrid locator service. */
  protected String itsHost;

  /** The Ice communicator to be used. */
  protected Communicator itsCommunicator = null;

  /** Name of the IceStorm topic for receiving new requests/keepalives. */
  protected String itsControlTopicName = null;

  /** The IceStorm topic for receiving new requests/keepalives. */
  protected TopicPrx itsControlTopic = null;

  /** The proxy for receiving new requests/keepalives. */
  protected PubSubControlPrx itsPubSubControl = null;

  /** Logger. */
  protected Logger itsLogger = Logger.getLogger(getClass().getName());

  /** Map of all clients currently subscribed to updates. */
  protected HashMap<String, PubSubClientInfo> itsClients = new HashMap<String, PubSubClientInfo>();

  /**
   * The maximum time between keep-alives before we consider a subscriber dead.
   */
  protected RelTime itsMaxKeepAliveTime = RelTime.factory(60000000); // TODO:
                                                                     // Config

  public PubSubManager() {
    // Read configuration for location of icegrid registry
    String tempstr = MonitorConfig.getProperty("PubSubLocatorPort");
    if (tempstr == null) {
      itsLogger.fatal("Require port for IceGrid locator service: PubSubLocatorPort");
      return;
    }
    try {
      itsPort = Integer.parseInt(tempstr);
    } catch (Exception e) {
      itsLogger.fatal("Unable to parse port number for IceGrid locator service: PubSubLocatorPort: Found \"" + tempstr + "\"");
      return;
    }
    itsHost = MonitorConfig.getProperty("PubSubLocatorHost");
    if (itsHost == null) {
      itsLogger.fatal("Require host name for IceGrid locator service: PubSubLocatorHost");
      return;
    }
    itsControlTopicName = MonitorConfig.getProperty("PubSubTopic");
    if (itsControlTopicName == null) {
      itsLogger.fatal("Require IceStorm topic name for listening for new subscriptions: PubSubTopic");
      return;
    }

    try {
      // TODO: Need to deal with this better. Probably have a thread to
      // manage reconnections.
      connect();
    } catch (Exception e) {
      itsLogger.fatal("Could not start " + e);
      return;
    }

    // Create thread to remove stale clients
    new AbandonedClientPurger().start();
  }

  /**
   * Connect to the IceStorm Topic so that we can start publishing data.
   */
  protected void connect() throws Exception {
    try {
      // Make Communicator
      disconnect();
      com.zeroc.Ice.Properties props = com.zeroc.Ice.Util.createProperties();
      String locator = "IceGrid/Locator:tcp -h " + itsHost + " -p " + itsPort;
      props.setProperty("Ice.Default.Locator", locator);
      props.setProperty("Ice.IPv6", "0");
      InitializationData id = new InitializationData();
      id.properties = props;
      itsCommunicator = com.zeroc.Ice.Util.initialize(id);

      // Obtain the topic or create
      TopicManagerPrx topicManager;
      ObjectPrx obj = itsCommunicator.stringToProxy("IceStorm/TopicManager@IceStorm.TopicManager");
      topicManager = TopicManagerPrx.checkedCast(obj);
      try {
        itsControlTopic = topicManager.retrieve(itsControlTopicName);
      } catch (NoSuchTopic e) {
        try {
          itsControlTopic = topicManager.create(itsControlTopicName);
        } catch (TopicExists e1) {
          itsControlTopic = topicManager.retrieve(itsControlTopicName);
        }
      }

      ObjectAdapter adapter = itsCommunicator.createObjectAdapterWithEndpoints("", "tcp");
      PubSubControlI monitor = new PubSubControlI();
      ObjectPrx proxy = adapter.addWithUUID(monitor).ice_twoway();
      itsControlTopic.subscribeAndGetPublisher(null, proxy);
      adapter.activate();
    } catch (Exception e) {
      itsLogger.error("(" + itsControlTopicName + "): While connecting: " + e);
      disconnect();
      throw e;
    }
  }

  /**
   * Disconnect from the Topic.
   */
  protected void disconnect() {
    if (itsControlTopic != null) {
      itsControlTopic.destroy();
    }

    itsControlTopic = null;
    itsPubSubControl = null;
  }
}
