package com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce;

import com.apple.irpt.df.jobs.kvs.dataWrite.schema.aggrDaily.Avro_aggrDaily_KVSDlyAggr0;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.apple.irpt.df.jobs.kvs.dataWrite.utility.KVSUtils.*;

public class KVSMapper1 extends Mapper<AvroKey<Avro_aggrDaily_KVSDlyAggr0>, NullWritable, Text, NullWritable> {

    private static final Logger LOGGER = Logger.getLogger(KVSMapper1.class);
    private Cache<String, String> cache;
    private BufferedReader brReader;
    private AvroMultipleOutputs mos;
    private Long maxRank = 0L;

    protected void setup(Context context) throws IOException,InterruptedException {
        mos = new AvroMultipleOutputs(context);
        cache = CacheBuilder.newBuilder().maximumSize(1000000).expireAfterWrite(2L, TimeUnit.HOURS).build();
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for (Path eachPath : cacheFilesLocal) {
            if (eachPath.getName().trim().equals("mapping.txt")) {
                loadMappingHashMap(new Path("/Users/ashish/IdeaProjects/KVSAggregationEncodingAvro/Input/mapping.txt"));
                //loadMappingHashMap(eachPath);
            }
        }
    }

    private void loadMappingHashMap(Path filePath) throws IOException {
        String strLineRead;
        Long rank;
        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));
            while ((strLineRead = brReader.readLine()) != null) {
                String mappingValues[] = strLineRead.split("\t");
                if (mappingValues.length > 1) {
                    cache.put(mappingValues[1], mappingValues[0]);
                    rank = Long.valueOf(mappingValues[0]);
                    maxRank = maxRank < rank ? rank : maxRank;
                }
            }
        } catch (Exception e) {
            LOGGER.info(e);
        } finally {
            if (brReader != null) {
                brReader.close();
            }
        }
    }

    public void map(AvroKey<Avro_aggrDaily_KVSDlyAggr0> key, NullWritable value, final Context context) throws IOException, InterruptedException {

        final Avro_aggrDaily_KVSDlyAggr0 aggr = key.datum();

        if (aggr != null) {
            String event_type = aggr.getEVENTTYPE() != null ? (aggr.getEVENTTYPE().equals("PUT") ? "1" : aggr.getEVENTTYPE().equals("GET") ? "0" : null) : null;
            String os = aggr.getOSVERSION() != null ? cache.getIfPresent(aggr.getOSVERSION().toString()) : null;
            String platform = aggr.getPLATFORM() != null ? cache.getIfPresent(aggr.getPLATFORM().toString()) : null;
            String application = aggr.getAPPLICATION() != null ? cache.getIfPresent(aggr.getAPPLICATION().toString()) : null;
            String kv_id = aggr.getKVID() != null ? cache.getIfPresent(aggr.getKVID().toString()) : null;
            String bundleId = aggr.getBUNDLEID() != null ? cache.getIfPresent(aggr.getBUNDLEID().toString()) : null;

            aggr.setEVENTTYPE(event_type);

            // Write unmapped keys to be encoded in next phase and Partial data
            if ((aggr.getBUNDLEID() != null && bundleId == null)
                    || (aggr.getOSVERSION() != null && os == null)
                    || (aggr.getPLATFORM() != null && platform == null)
                    || (aggr.getAPPLICATION() != null && application == null)
                    || (aggr.getKVID() != null && kv_id == null)) {

                if (bundleId == null && aggr.getBUNDLEID() != null) {
                    writeUnmappedKey(context, aggr.getBUNDLEID().toString());
                } else {
                    aggr.setBUNDLEID(bundleId);
                }
                if (os == null && aggr.getOSVERSION() != null) {
                    writeUnmappedKey(context, aggr.getOSVERSION().toString());
                } else {
                    aggr.setOSVERSION(os);
                }
                if (platform == null && aggr.getPLATFORM() != null) {
                    writeUnmappedKey(context, aggr.getPLATFORM().toString());
                } else {
                    aggr.setPLATFORM(platform);
                }
                if (kv_id == null && aggr.getKVID() != null) {
                    writeUnmappedKey(context, aggr.getKVID().toString());
                } else {
                    aggr.setKVID(kv_id);
                }
                if (application == null) {
                    writeUnmappedKey(context, (String) aggr.getAPPLICATION());
                } else {
                    aggr.setAPPLICATION(application);
                }

                writePartialEncodedData(context, aggr);

            } else {
                aggr.setOSVERSION(os);
                aggr.setBUNDLEID(bundleId);
                aggr.setKVID(kv_id);
                aggr.setAPPLICATION(application);
                aggr.setPLATFORM(platform);

                writeFullEncodedData(context, aggr);

            }
        }
    }

    private void writeUnmappedKey(final Context context, final String field) throws IOException, InterruptedException {
        context.write(new Text(field + "\t" + maxRank), NullWritable.get());
    }

    private void writePartialEncodedData(final Context context, final Avro_aggrDaily_KVSDlyAggr0 aggr) throws IOException, InterruptedException {
        mos.write(PARTIAL_ENCODED_DATA, new AvroKey<Avro_aggrDaily_KVSDlyAggr0>(aggr), NullWritable.get(), context.getConfiguration().get(PARTIAL_ENCODED_DATA));
    }

    private void writeFullEncodedData(final Context context, final Avro_aggrDaily_KVSDlyAggr0 aggr) throws IOException, InterruptedException {
        mos.write(INITIAL_ENCODED_DATA, new AvroKey<Avro_aggrDaily_KVSDlyAggr0>(aggr), NullWritable.get(), context.getConfiguration().get(INITIAL_ENCODED_DATA));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

}
