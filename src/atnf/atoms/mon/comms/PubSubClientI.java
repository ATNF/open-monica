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

import org.apache.log4j.Logger;

import java.util.*;
import atnf.atoms.time.*;
import atnf.atoms.mon.*;
import atnf.atoms.mon.util.MonitorConfig;

import com.zeroc.Ice.Current;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.InitializationData;
import com.zeroc.Ice.ObjectPrx;
import com.zeroc.Ice.ObjectAdapter;

import com.zeroc.IceStorm.*;

/**
 * @author David Brodrick
 */
public class PubSubClientI implements PubSubClient {
    /** Name of the topic we receive updates on. */
    protected String itsTopicName;

    /** Topic for receiving updates. */
    protected TopicPrx itsTopic = null;

    /** Name of the topic used to subscribe to updates and send keep-alives. */
    protected String itsControlTopicName;

    /** Topic used to subscribe to updates and send keep-alives. */
    protected TopicPrx itsControlTopic;

    /** Proxy to the server. */
    protected PubSubControlPrx itsPubSubControl;

    /** Minimum time between keep-alives. */
    protected RelTime itsKeepAliveTime = RelTime.factory(30000000);

    /** TCP port number for IceGrid locator service. */
    protected int itsPort;

    /** Host name for IceGrid locator service. */
    protected String itsHost;

    /** The Ice communicator to be used. */
    protected Communicator itsCommunicator;

    /** The listener who wants to receive updates. */
    protected PointListener itsListener;

    /** Logger. */
    protected Logger itsLogger = Logger.getLogger(getClass().getName());

    /** Constructor. */
    public PubSubClientI(String lochost, int locport, String contopic, PointListener client) throws Exception
    {
        itsHost = lochost;
        itsPort = locport;
        itsControlTopicName = contopic;
        itsListener = client;

        // Connect to control/subscription topic
        connect();

        // Create keep-alive thread
        new Thread() {
            public void run()
            {
                while (true) {
                    if (itsPubSubControl != null && itsTopicName != null) {
                        itsPubSubControl.keepalive(itsTopicName);
                    }
                    try {
                        itsKeepAliveTime.sleep();
                    } catch (Exception e) {
                    }
                }
            }
        }.start();
    }

    /** Create Communicator and get proxy to the server. */
    protected void connect() throws Exception
    {
        try {
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

            itsControlTopic = topicManager.retrieve(itsControlTopicName);

            ObjectPrx pub = itsControlTopic.getPublisher().ice_twoway();
            itsPubSubControl = PubSubControlPrx.uncheckedCast(pub);

            itsLogger.debug("Connect to control topic okay");
        } catch (Exception e) {
            itsLogger.error("(" + itsControlTopicName + "): While connecting: " + e);
            throw e;
        }
    }

    /** Subscribe to the given points. */
    public void subscribe(String[] pointnames) throws Exception
    {
        // Obtain the topic or create
        TopicManagerPrx topicManager;
        ObjectPrx obj = itsCommunicator.stringToProxy("IceStorm/TopicManager@IceStorm.TopicManager");
        topicManager = TopicManagerPrx.checkedCast(obj);

        // Create a unique topic name
        // TODO:
        itsTopicName = "MoniCA." + UUID.randomUUID().toString();

        try {
            itsTopic = topicManager.retrieve(itsTopicName);
        } catch (NoSuchTopic e) {
            try {
                itsTopic = topicManager.create(itsTopicName);
            } catch (TopicExists e1) {
                itsTopic = topicManager.retrieve(itsTopicName);
            }
        }

        ObjectAdapter adapter = itsCommunicator.createObjectAdapterWithEndpoints("", "tcp");
        ObjectPrx proxy = adapter.addWithUUID(this).ice_twoway();
        itsTopic.subscribeAndGetPublisher(null, proxy);

        adapter.activate();

        PubSubRequest req = new PubSubRequest();
        req.topicname = itsTopicName;
        req.pointnames = pointnames;

        itsPubSubControl.subscribe(req);
    }

    /** Callback from Ice when new data is published on our topic. */
    public void updateData(PointDataIce[] newdataice, Current curr)
    {
        Vector<PointData> newdata = MoniCAIceUtil.getPointDataFromIce(newdataice);
        for (int i = 0; i < newdata.size(); i++) {
            itsListener.onPointEvent(this, new PointEvent(this, newdata.get(i), false));
        }
    }

    /** Test program: Subscribe to a specified point. */
    public static final void main(String[] args)
    {
        if (args.length != 4) {
            System.err.println("USAGE: Requires the following arguments:");
            System.err.println("\tHost name for locator service.");
            System.err.println("\tPort number for locator service.");
            System.err.println("\tName of control topic for making subscriptions.");
            System.err.println("\tName of point to subscribe to.");
            System.exit(1);
        }

        String host = args[0];
        int port = 9999;
        try {
            port = Integer.parseInt(args[1]);
        } catch (Exception e) {
            System.err.println("ERROR: Could not parse port number for locator service");
            System.exit(1);
        }
        String topic = args[2];
        String pointname = args[3];

        PointListener client = new PointListener() {
            /** Callback for when a listened-to point updates. */
            public void onPointEvent(Object source, final PointEvent evt)
            {
                System.out.println(evt.getPointData());
            }
        };

        try {
            PubSubClientI test = new PubSubClientI(host, port, topic, client);
            test.subscribe(new String[] { pointname });
        } catch (Exception e) {
            System.err.println("ERROR: " + e);
            System.exit(1);
        }
    }
}
