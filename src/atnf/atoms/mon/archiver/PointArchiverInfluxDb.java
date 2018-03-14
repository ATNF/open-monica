// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.archiver;

import atnf.atoms.mon.PointData;
import atnf.atoms.mon.PointDescription;
import atnf.atoms.mon.archiver.influx.OrderedProperties;
import atnf.atoms.mon.archiver.influx.TagExtractor;
import atnf.atoms.mon.util.MonitorConfig;
import atnf.atoms.time.AbsTime;
import atnf.atoms.time.RelTime;
import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * #%L
 * CSIRO InfluxDB Archiver Plugin
 * %%
 * Copyright (C) 2018 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * <p>
 * <p>
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

public class PointArchiverInfluxDb extends PointArchiver {

    /**
     * InfluxDB database name
     */
    private final static String theirDatabaseName = MonitorConfig.getProperty("InfluxDB", "testing");

    /**
     * Global retention policy
     */
    private final static String theirRetentionPolicy = MonitorConfig.getProperty("InfluxRetentionPolicy", "autogen");

    /**
     * The URL to connect to the server/database.
     */
    private final static String theirURL = MonitorConfig.getProperty("InfluxURL", "http://localhost:8086");

    /**
     * Influx username
     */
    private final static String theirUsername = MonitorConfig.getProperty("InfluxUsername", "admin");

    /**
     * Influx password
     */
    private final static String theirPassword = MonitorConfig.getProperty("InfluxPassword", "admin");

    /**
     * Tag file location
     */
    private final static String theirTagFilePath = MonitorConfig.getProperty("InfluxTagMap", "");

    /**
     * Maximum size of batch update (number of samples)
     */
    @SuppressWarnings("CanBeFinal")
    private static long theirInfluxBatchSize = 10000;

    /**
     * Maximum age of batch before flushing (in milliseconds)
     */
    @SuppressWarnings("CanBeFinal")
    private static long theirInfluxBatchAge = 10000;

    /**
     * Toggle for ASCII archiver
     */
    @SuppressWarnings("CanBeFinal")
    private static Boolean theirChainToASCII = false;

    /**
     * Tag Extractor for generating Influxdb Tags
     */
    private Map<String, TagExtractor> itsTags = null;

    /**
     * Queue of batch updates to send to InfluxDB server
     */
    private final LinkedBlockingDeque<BatchPoints> itsBatchQueue = new LinkedBlockingDeque<BatchPoints>(1000);

    /**
     * The current batch being created
     */
    private BatchPoints itsCurrentBatch = null;

    /**
     * The oldest timestamp (epoch) in the current batch
     */
    private long itsOldestPointTime = 0;

    /**
     * starting time of the current batch for computing batch age
     */
    private long itsBatchStart = 0;

    /**
     * batch for metadata (long point description and units)
     */
    private BatchPoints itsMetadataBatch = null;

    /**
     * Chained ASCII archiver
     */
    private PointArchiverASCII itsChainedArchiver = null;

    class InfluxSeries {
        Map<String, String> tags;
        String measurement;
        String field;
        AbsTime lastTimestamp = AbsTime.ASAP;
    }

    private final Map<PointDescription, InfluxSeries> itsInfluxMap = new HashMap<PointDescription, InfluxSeries>();

    /**
     * The connection to the InfluxDB server.
     */
    private InfluxDB itsInfluxDB = null;

    private int itsUnmappedPoints = 0;

    /* Static block to parse parameters. */
    static {
        try {
            theirInfluxBatchSize = Integer.parseInt(MonitorConfig.getProperty("InfluxBatchSize", "1000"));
        } catch (Exception e) {
            Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxBatchSize configuration parameter: " + e);
        }

        try {
            theirInfluxBatchAge = 1000 * Integer.parseInt(MonitorConfig.getProperty("InfluxBatchAge", "1"));
        } catch (Exception e) {
            Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxBatchAge configuration parameter: " + e);
        }

        try {
            theirChainToASCII = Boolean.parseBoolean(MonitorConfig.getProperty("InfluxChainToASCII", "false"));
        } catch (Exception e) {
            Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxChainToASCII : " + e);
        }
    }

    @SuppressWarnings("unused")
    public PointArchiverInfluxDb() {
        super();

        // load tags
        itsLogger.info("loading InfluxDB mappings from " + theirTagFilePath);
        final OrderedProperties tagProperties = OrderedProperties.getInstance(theirTagFilePath);
        itsTags = new LinkedHashMap<String, TagExtractor>();
        for (Object o : tagProperties.getProperties().entrySet()) {
            Map.Entry pair = (Map.Entry) o;
            itsTags.put((String) pair.getKey(), TagExtractor.fromString((String) pair.getValue()));
        }

        // chain the ASCII archiver
        if (theirChainToASCII) {
            itsChainedArchiver = new PointArchiverASCII();
        }
        try {
            influxThread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a influxDB connection
     *
     */
    private Boolean setUp() {
        try {
            itsInfluxDB = InfluxDBFactory.connect(theirURL, theirUsername, theirPassword);
            boolean influxDBstarted = false;
            do {
                Pong response;
                response = itsInfluxDB.ping();
                if (!response.getVersion().equalsIgnoreCase("unknown")) {
                    influxDBstarted = true;
                }
                Thread.sleep(100L);
            } while (!influxDBstarted);
            itsInfluxDB.setLogLevel(LogLevel.NONE);
            itsLogger.info("Connected to " + theirURL + " InfluxDB Version: " + this.itsInfluxDB.version());
            if (isConnected()) {
                // check for database
                if (!itsInfluxDB.describeDatabases().contains(theirDatabaseName)) {
                    itsLogger.warn("Influx database " + theirDatabaseName + " not found");
                    shutdown();
                    return false;
                }
                if (!itsInfluxDB.describeDatabases().contains(theirDatabaseName + "_metadata")) {
                    itsLogger.warn("Influx database " + theirDatabaseName + "_metadata not found");
                    shutdown();
                    return false;
                }
            }
            return true;
        }
        catch (Exception e) {
            itsLogger.warn("Can't connect to Influx server " + theirURL);
            itsInfluxDB = null;
        }
        return false;
    }

    private void shutdown() {
        itsLogger.info("closing influxdb connection");
        if (isConnected()) {
            itsInfluxDB.close();
            itsInfluxDB = null;
        }
        itsLogger.info("done shutting down");
    }

    /**
     * Check if we are connected to the server and reconnect if required.
     *
     * @return True if connected (or reconnected). False if not connected.
     */
    private boolean isConnected() {
        return itsInfluxDB != null;
    }

    /**
     * Ping influxdb
     *
     * print the version and response time of the influxDB object's database
     */
    @SuppressWarnings("unused")
    protected void Ping() {
        Pong result = this.itsInfluxDB.ping();
        String version = result.getVersion();
        System.out.println("Version: " + version + " | Response: " + result + "ms");
        System.out.println(itsInfluxDB.describeDatabases());
    }

    /**
     * Extract data from the archive with no undersampling.
     *
     * @param pm    Point to extract data for.
     * @param start Earliest time in the range of interest.
     * @param end   Most recent time in the range of interest.
     * @return Vector containing all data for the point over the time range.
     */
    protected Vector<PointData> extractDeep(PointDescription pm, AbsTime start, AbsTime end) {
        if (itsChainedArchiver == null) {
            return null;
        }
        return itsChainedArchiver.extractDeep(pm, start, end);
    }

    /**
     * Main loop for the archiving thread. Override from Archiver class
     * due to need for archiving to InfluxDB in realtime.
     */
    public void run() {
        setName("Point Archiver");

        if (itsChainedArchiver != null) {
            itsLogger.info("starting chained ASCII archiver");
            itsChainedArchiver.start();
        }

        RelTime sleeptime1 = RelTime.factory(50000);
        RelTime sleeptime2 = RelTime.factory(1000);
        while (true) {
            boolean flushing = false;
            if (itsShuttingDown) {
                flushing = true;
            }

            AbsTime cutoff = (new AbsTime()).add(theirMaxAge);
            int counter = 0;
            Enumeration<PointDescription> keys = itsBuffer.keys();
            try {
                while (keys.hasMoreElements()) {
                    PointDescription pm = keys.nextElement();
                    if (pm == null) {
                        continue;
                    }

                    Vector<PointData> thisdata = itsBuffer.get(pm);
                    if (thisdata == null || thisdata.isEmpty()) {
                        // No data to be archived
                        continue;
                    }

                    //noinspection StatementWithEmptyBody
                    if (!itsShuttingDown) {
                        // Add small offsets based on hash of point name.
                        // This prevents bulk points all being flushed together each time.
                        int namehash = pm.getFullName().hashCode();
                        int minnumrecs = theirMaxRecordCount + (namehash % theirRecordCountOffset);
                        AbsTime cutoff2 = cutoff.add(namehash % theirMaxAgeOffset);
                        // archive immediately to influx, it will handle batching
                        saveNow(pm, thisdata);

                        if (thisdata.size() < minnumrecs && thisdata.lastElement().getTimestamp().isAfter(cutoff2)) {
                            // Point does not meet any criteria for writing to the archive at this time
                            continue;
                        }
                    } else {
                        // itsLogger.debug("Flushing " + thisdata.size() + " records for " + pm.getFullName() + " because shutdown requested");
                    }

                    if (itsChainedArchiver == null) {
                        thisdata.clear();
                    } else {
                        HashSet<String> archivingStatus = itsChainedArchiver.itsBeingArchived;
                        // TODO need to fix in PointArchiver
                        // noinspection SynchronizeOnNonFinalField
                        synchronized (itsChainedArchiver.itsBeingArchived) {
                            if (archivingStatus.contains(pm.getFullName())) {
                                // Point is already being archived
                                //itsLogger.warn(pm.getFullName() + " is already being archived");
                                continue;
                            } else {
                                // Flag that the point is now being archived
                                itsBeingArchived.add(pm.getFullName());
                            }
                        }
                        itsChainedArchiver.saveNow(pm, thisdata);
                    }

                    try {
                        sleeptime2.sleep();
                    } catch (Exception e) {
                        itsLogger.warn("exception caught: " + e);
                    }
                    counter++;
                }
            } catch (Exception e) {
                itsLogger.error("While archiving: " + e);
                e.printStackTrace();
            }
            // if (counter > 0) {
            // itsLogger.debug("###### Archived/flagged " + counter + " points");
            // }
            if (itsShuttingDown && flushing) {
                // We've just flushed the full archive
                itsFlushComplete = true;
                break;
            }
            try {
                sleeptime1.sleep();
            } catch (Exception e) {
                itsLogger.warn("exception caught: " + e);
            }
        }
        itsLogger.info("shutting down archiver");
    }

    /**
     * Method to do the actual archiving.
     *
     * @param pm The point whos data we wish to archive.
     * @param pd Vector of data to be archived.
     */
    protected void saveNow(PointDescription pm, Vector<PointData> pd) {
        if (itsShuttingDown) {
            return;
        }

        if (!itsInfluxMap.containsKey(pm)) {
            // map the MoniCA key to the Influx series name
            InfluxSeries seriesInfo = new InfluxSeries();
            String pointName = pm.getSource() + "." + pm.getName();
            seriesInfo.tags = new HashMap<String, String>();
            final TagExtractor.Holder nameHolder = new TagExtractor.Holder(pointName);
            for (Object o : itsTags.entrySet()) {
                Map.Entry pair = (Map.Entry) o;
                String key = (String) pair.getKey();
                TagExtractor te = (TagExtractor) pair.getValue();
                String result = te.apply(nameHolder);
                if (result != null) {
                    seriesInfo.tags.put(key, result);
                }
            }

            // if there are no explicit tags found then point has not
            // been mapped.  In this case just map the MoniCA source
            // to a tag
            if (seriesInfo.tags.size() == 0) {
                TagExtractor te = TagExtractor.fromString(pm.getSource());
                seriesInfo.tags.put("source", te.apply(nameHolder));
                itsLogger.warn("point " + pm.getFullName() + " not explicitly mapped to influx");
                ++itsUnmappedPoints;
            }

            // add unit as a tag
            String pointUnits = pm.getUnits();
            if (pointUnits != null && pointUnits.length() > 0) {
                seriesInfo.tags.put("units", pointUnits);
            }

            // field name is last part of MoniCA point name
            // measurement name is the first part
            String name = nameHolder.get();
            int valueNameSepIndex = name.lastIndexOf(".");
            seriesInfo.measurement = name.substring(0, valueNameSepIndex);
            seriesInfo.field = name.substring(valueNameSepIndex + 1);
            itsInfluxMap.put(pm, seriesInfo);

            try {
                if (itsMetadataBatch == null) {
                    itsMetadataBatch = BatchPoints
                            .database(theirDatabaseName + "_metadata")
                            .retentionPolicy(theirRetentionPolicy)
                            .consistency(InfluxDB.ConsistencyLevel.ALL)
                            .build();
                }
                HashMap<String, String> metadataTags = new HashMap<String, String>(seriesInfo.tags);
                metadataTags.remove("antenna");
                String fieldValue = pm.getLongDesc() + "," + pm.getUnits() + "," + pm.getInputTransactionString();
                Point metadata = Point.measurement(seriesInfo.measurement)
                        .time(0, TimeUnit.MILLISECONDS)
                        .addField(seriesInfo.field, fieldValue)
                        .tag(metadataTags)
                        .build();
                itsMetadataBatch.point(metadata);
                if (itsMetadataBatch.getPoints().size() >= theirInfluxBatchSize) {
                    itsLogger.debug("adding metadata");
                    itsBatchQueue.addLast(itsMetadataBatch);
                    itsMetadataBatch = null;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (itsCurrentBatch == null) {
            itsCurrentBatch = BatchPoints
                    .database(theirDatabaseName)
                    .retentionPolicy(theirRetentionPolicy)
                    .consistency(InfluxDB.ConsistencyLevel.ALL)
                    .build();
            itsBatchStart = System.nanoTime();
        }

        Long pointTime;
        InfluxSeries seriesInfo = itsInfluxMap.get(pm);
        if (seriesInfo == null) {
            itsLogger.error("map not ready");
            return;
        }

        // TODO foreach leads to comodification error
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < pd.size(); i++) {
            try { //extract point data and metadata to write to influx
                PointData pointData = pd.get(i);

                if (pointData.getTimestamp().isBeforeOrEquals(seriesInfo.lastTimestamp)) {
                    // skip points that we have already batched to Influx
                    //itsLogger.debug(pm.getFullName() + " skipping point " + i);
                    continue;
                }

                // pointTime in a BAT time so convert to epoch for Influx
                Date pointDate = pointData.getTimestamp().getAsDate();
                if (pointDate != null) {
                    pointTime = pointDate.getTime();
                    if (pointTime < itsOldestPointTime || itsOldestPointTime == 0) {
                        itsOldestPointTime = pointTime;
                    }
                    Point.Builder pb = Point.measurement(seriesInfo.measurement)
                            .time(pointTime, TimeUnit.MILLISECONDS)
                            .tag(seriesInfo.tags);

                    Object pointValueObj = pointData.getData();
                    if (pointData.getData() instanceof Double) {
                        pb.addField(seriesInfo.field, ((Number) pointValueObj).doubleValue());
                    } else if (pointData.getData() instanceof Float) {
                        pb.addField(seriesInfo.field, ((Number) pointValueObj).floatValue());
                    } else if (pointData.getData() instanceof Integer) {
                        pb.addField(seriesInfo.field, ((Number) pointValueObj).intValue());
                    } else if (pointValueObj instanceof String) {
                        pb.addField(seriesInfo.field, (String) pointValueObj);
                    }
                    itsCurrentBatch.point(pb.build());
                }
            } catch (Exception a) {
                a.printStackTrace();
            }
        }
        seriesInfo.lastTimestamp = pd.lastElement().getTimestamp();

        long elapsedPoint = System.currentTimeMillis() - itsOldestPointTime;
        long batchAge = (System.nanoTime() - itsBatchStart) / 1000000;
        if (itsCurrentBatch.getPoints().size() >= theirInfluxBatchSize
                || (itsCurrentBatch.getPoints().size() > 0 && batchAge > theirInfluxBatchAge)) {
            try {
                if (itsMetadataBatch != null) {
                    // add any remaining metadata before we archive any points
                    itsLogger.debug("adding metadata " + itsMetadataBatch.getPoints().size());
                    itsBatchQueue.addLast(itsMetadataBatch);
                    itsMetadataBatch = null;
                }
                itsLogger.debug("adding batch to queue " + itsCurrentBatch.getPoints().size() + " batch age " + batchAge + " point age " + elapsedPoint + "ms");
                itsBatchQueue.addLast(itsCurrentBatch);
                itsCurrentBatch = null;
                itsOldestPointTime = 0;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private final Thread influxThread;

    {
        influxThread = new Thread(new Runnable() {
            @Override
            public void run() {
                long rateStart = System.nanoTime();
                long pointsPerMinute = 0;
                long rateElapsed;

                while (true) {
                    try {
                        // connect / reconnect to Influx Server
                        if (!isConnected() && !setUp()) {
                            itsLogger.warn("not connected to influx, " + itsBatchQueue.size() + " batches waiting");
                            sleep(60000);
                            continue;
                        }
                        BatchPoints batchPoints = itsBatchQueue.takeFirst();
                        if (itsShuttingDown) {
                            itsLogger.info("shutting down influx thread");
                            break;
                        }
                        long startTime = System.nanoTime();
                        try {
                            itsInfluxDB.write(batchPoints);
                        } catch (Exception e) {
                            itsLogger.warn("write to influx failed, try to reconnect");
                            shutdown();
                            // put the points back in the queue
                            if (itsBatchQueue.size() < 1000) {
                                itsBatchQueue.addFirst(batchPoints);
                            } else {
                                itsLogger.warn("queue full, dropping batch");
                            }
                        }
                        long elapsed = (System.nanoTime() - startTime) / 1000000;
                        itsLogger.debug("sent " + batchPoints.getPoints().size()
                                + " to influxdb in " + elapsed + "ms depth " + itsBatchQueue.size());
                        rateElapsed = (System.nanoTime() - rateStart) / 1000000;
                        pointsPerMinute += batchPoints.getPoints().size();
                        if (rateElapsed >= 60000) {
                            double rate = 1.0 * pointsPerMinute / 60;
                            itsLogger.info(String.format("ingest rate %.1f points per second", rate));
                            rateStart = System.nanoTime();
                            pointsPerMinute = 0;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
                shutdown();
            }
        });
    }

    /**
     * Tell the archiver that MoniCA needs to shut down so that unflushed data can be written out.
     */
    public void flushArchive() {
        itsLogger.info("Flushing archiver");
        itsShuttingDown = true;
        BatchPoints terminate = BatchPoints
                .database("terminate")
                .retentionPolicy(theirRetentionPolicy)
                .consistency(InfluxDB.ConsistencyLevel.ALL)
                .build();
        itsBatchQueue.addLast(terminate);
        if (itsChainedArchiver != null) {
            itsChainedArchiver.flushArchive();
        }
        while (!itsFlushComplete) {
            try {
                RelTime.factory(100000).sleep();
            } catch (Exception e) {
                itsLogger.warn("exception caught: " + e);
            }
        }
    }

    /**
     * Return the last update which precedes the specified time. We interpret 'precedes' to mean data_time<=req_time.
     *
     * @param pm Point to extract data for.
     * @param ts Find data preceding this timestamp.
     * @return PointData for preceding update or null if none found.
     */
    protected PointData getPrecedingDeep(PointDescription pm, AbsTime ts) {
        if (itsChainedArchiver == null) {
            return null;
        }
        return itsChainedArchiver.getPrecedingDeep(pm, ts);
    }

    /**
     * Return the first update which follows the specified time. We interpret 'follows' to mean data_time>=req_time.
     *
     * @param pm Point to extract data for.
     * @param ts Find data following this timestamp.
     * @return PointData for following update or null if none found.
     */
    protected PointData getFollowingDeep(PointDescription pm, AbsTime ts) {
        if (itsChainedArchiver == null) {
            return null;
        }
        return itsChainedArchiver.getFollowingDeep(pm, ts);
    }

    /**
     * Purge all data for the given point that is older than the specified age in days.
     *
     * @param pd The point whos data we wish to purge.
     */
    protected void purgeOldData(PointDescription pd) {
        if (itsChainedArchiver == null) {
            return;
        }
        itsChainedArchiver.purgeOldData(pd);
    }

}


