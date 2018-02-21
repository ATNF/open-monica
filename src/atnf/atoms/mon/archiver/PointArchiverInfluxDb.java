package atnf.atoms.mon.archiver;

import atnf.atoms.mon.PointData;
import atnf.atoms.mon.PointDescription;
import atnf.atoms.mon.util.MonitorConfig;
import atnf.atoms.time.AbsTime;
import atnf.atoms.time.RelTime;
import au.csiro.pbdava.diamonica.etl.influx.TagExtractor;
import au.csiro.pbdava.diamonica.etl.influx.OrderedProperties;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.influxdb.InfluxDB.LogLevel;
import org.apache.log4j.Logger;

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
    private static String theirDatabaseName = MonitorConfig.getProperty("InfluxDB", "testing");

    /**
     * Global retention policy
     */
    private static String theirRetentionPolicy = MonitorConfig.getProperty("InfluxRetensionPolicy", "autogen");

    /**
     * The URL to connect to the server/database.
     */
    private static String theirURL = MonitorConfig.getProperty("InfluxURL", "http://localhost:8086");

    /**
     * Influx username
     */
    private static String theirUsername = MonitorConfig.getProperty("InfluxUsername", "admin");

    /**
     * Influx password
     */
    private static String theirPassword = MonitorConfig.getProperty("InfluxPassword", "admin");

    /**
     * Tag file location
     */
    private static String theirTagFilePath = MonitorConfig.getProperty("InfluxTagMap", "");

    /**
     * Maximum size of batch update (number of samples)
     */
    private static long theirInfluxBatchSize = 10000;

    /**
     * Maximum age of batch before flushing (in milliseconds)
     */
    private static long theirInfluxBatchAge = 10000;

    /**
     * Toggle for ASCII archiver
     */
    private static Boolean theirChainToASCII = false;


    /**
     * Tag Extractor for generating Influxdb Tags
     */
    private Map<String, TagExtractor> tags = null;

    /**
     * Queue of batch updates to send to InfluxDB server
     */
    private BlockingQueue<BatchPoints> itsBatchQueue = new LinkedBlockingQueue<BatchPoints>();

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
        AbsTime lastTimestamp = AbsTime.NEVER;
    }

    Map<PointDescription, InfluxSeries> itsInfluxMap = new HashMap<>();

    /**
     * The connection to the InfluxDB server.
     */
    private InfluxDB influxDB = null;


    /** Static block to parse flush parameters. */
    static {
        try {
            long param = Integer.parseInt(MonitorConfig.getProperty("InfluxBatchSize", "1000"));
            theirInfluxBatchSize = param;
        } catch (Exception e) {
            Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxBatchSize configuration parameter: " + e);
        }

        try {
            long param = 1000 * Integer.parseInt(MonitorConfig.getProperty("InfluxBatchAge", "1"));
            theirInfluxBatchAge = param;
        } catch (Exception e) {
            Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxBatchAge configuration parameter: " + e);
        }

        try {
            Boolean param = Boolean.parseBoolean(MonitorConfig.getProperty("InfluxChainToASCII", "false"));
            theirChainToASCII = param;
        } catch (Exception e) {
            Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing InfluxChainToASCII : " + e);
        }
    }

    public PointArchiverInfluxDb() {
        super();
        // chain the ASCII archiver
        if (theirChainToASCII) {
            itsLogger.info("chaining to ASCII archiver");
            itsChainedArchiver = new PointArchiverASCII();
        }
        try {
            setUp();
            runThread.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a influxDB connection
     *
     * @throws InterruptedException
     * @throws IOException
     */
    private void setUp() throws InterruptedException, IOException {
        this.influxDB = InfluxDBFactory.connect(theirURL, theirUsername, theirPassword);
        boolean influxDBstarted = false;
        do {
            Pong response;
            try {
                response = this.influxDB.ping();
                if (!response.getVersion().equalsIgnoreCase("unknown")) {
                    influxDBstarted = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(100L);
        } while (!influxDBstarted);
        this.influxDB.setLogLevel(LogLevel.NONE);
        itsLogger.info("Connected to InfluxDB Version: " + this.influxDB.version());

        // load tags
        final OrderedProperties tagProperties = OrderedProperties.getInstance(theirTagFilePath);
        tags = new LinkedHashMap<String, TagExtractor>();
        tagProperties.getProperties().forEach((k, v) -> tags.put(k, TagExtractor.fromString(v)));

        // create databases
        influxDB.createDatabase(theirDatabaseName);

        // we create the metadata from scratch each time
        influxDB.deleteDatabase(theirDatabaseName + "_metadata");
        influxDB.createDatabase(theirDatabaseName + "_metadata");
    }

    /**
     * Check if we are connected to the server and reconnect if required.
     *
     * @return True if connected (or reconnected). False if not connected.
     */
    protected boolean isConnected() {
        return influxDB != null;
    }

    /**
     * Ping influxdb
     *
     * @print The version and response time of the influxDB object's database
     */
    protected void Ping() {
        Pong result = this.influxDB.ping();
        String version = result.getVersion();
        System.out.println("Version: " + version + " | Response: " + result + "ms");
        System.out.println(influxDB.describeDatabases());
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
     * due to need to archiving in realtime.
     */
    public void run() {
        setName("Point Archiver");

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
                    Vector<PointData> thisdata = null;
                    PointDescription pm = keys.nextElement();
                    if (pm == null) {
                        continue;
                    }
                    thisdata = itsBuffer.get(pm);
                    if (thisdata == null || thisdata.isEmpty()) {
                        // No data to be archived
                        continue;
                    }

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
                        synchronized (archivingStatus) {
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
            }
        }
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
        long startTime = System.nanoTime();

        if (!itsInfluxMap.containsKey(pm)) {
            // map the MoniCA key to the Influx series name
            InfluxSeries seriesInfo = new InfluxSeries();
            String pointName = pm.getSource() + "." + pm.getName();
            seriesInfo.tags = new HashMap<String, String>();
            final TagExtractor.Holder nameHolder = new TagExtractor.Holder(pointName);
            tags.forEach((n, te) -> te.apply(nameHolder).ifPresent(v -> seriesInfo.tags.put(n, v)));

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
                HashMap<String, String> metadataTags = new HashMap<>(seriesInfo.tags);
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
                    itsBatchQueue.add(itsMetadataBatch);
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
            itsBatchStart = Instant.now().toEpochMilli();
        }

        Long pointTime;
        InfluxSeries seriesInfo = itsInfluxMap.get(pm);
        if (seriesInfo == null) {
            itsLogger.error("map not ready");
            return;
        }
        for (int i = 0; i < pd.size(); i++) {
            try { //extract point data and metadata to write to influx
                PointData pointData = pd.get(i);

                if (pointData.getTimestamp().isBeforeOrEquals(seriesInfo.lastTimestamp)) {
                    // skip points that we have already batched to Influx
                    //itsLogger.debug(pm.getFullName() + " skipping point " + i);
                    continue;
                }

                // pointTime in a BAT time so convert to epoch for Influx
                pointTime = pointData.getTimestamp().getAsDate().getTime();
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
            } catch (Exception a) {
                a.printStackTrace();
            }
        }
        seriesInfo.lastTimestamp = pd.lastElement().getTimestamp();

        long elapsedPoint = Instant.now().toEpochMilli() - itsOldestPointTime;
        long batchAge = Instant.now().toEpochMilli() - itsBatchStart;
        if (itsCurrentBatch.getPoints().size() >= theirInfluxBatchSize
                || batchAge > theirInfluxBatchAge) {
            try {
                if (itsMetadataBatch != null) {
                    itsLogger.debug("adding last metadata " + itsMetadataBatch.getPoints().size());
                    itsBatchQueue.add(itsMetadataBatch);
                    itsMetadataBatch = null;
                }
                itsLogger.debug("adding batch to queue " + itsCurrentBatch.getPoints().size() + " points age " + elapsedPoint + "ms");
                itsBatchQueue.put(itsCurrentBatch);
                itsCurrentBatch = null;
                itsOldestPointTime = 0;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    Thread runThread = new Thread(new Runnable() {
        @Override
        public void run() {
            long rateStart = Instant.now().toEpochMilli();
            long pointsPerMinute = 0;
            long rateElapsed = 0;
            while (!itsShuttingDown) {
                try {
                    BatchPoints batchPoints = itsBatchQueue.take();
                    long startTime = System.nanoTime();
                    influxDB.write(batchPoints);
                    long elapsed = (System.nanoTime() - startTime) / 1000000;
                    long elapsedPoint = Instant.now().toEpochMilli() - itsOldestPointTime;
                    itsLogger.debug("sent " + batchPoints.getPoints().size()
                            + " to influxdb in " + elapsed + "ms depth " + itsBatchQueue.size());
                    rateElapsed = Instant.now().toEpochMilli() - rateStart;
                    pointsPerMinute += batchPoints.getPoints().size();
                    if (rateElapsed >= 60000) {
                        double rate = 1.0 * pointsPerMinute / 60;
                        itsLogger.info(String.format("ingest rate %.1f points per second", rate));
                        rateStart = Instant.now().toEpochMilli();
                        pointsPerMinute = 0;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            itsLogger.info("closing influxdb connection");
            influxDB.close();
            influxDB = null;
        }
    });

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


