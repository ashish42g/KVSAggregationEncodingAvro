package com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce;

import com.apple.irpt.df.jobs.kvs.dataWrite.schema.aggrDaily.Avro_aggrDaily_KVSDlyAggr0;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.apple.irpt.df.jobs.kvs.dataWrite.utility.KVSUtils.*;

public class KVSMapper2 extends Mapper<AvroKey<Avro_aggrDaily_KVSDlyAggr0>, NullWritable, AvroKey<Avro_aggrDaily_KVSDlyAggr0>, NullWritable> {

    private static final Logger LOGGER = Logger.getLogger(KVSMapper2.class);
    private Cache<String, String> cache;
    private BufferedReader brReader;
    AvroMultipleOutputs mos;

    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new AvroMultipleOutputs(context);
        cache = CacheBuilder.newBuilder().maximumSize(1000000).expireAfterWrite(2L, TimeUnit.HOURS).build();
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        for (Path eachPath : cacheFilesLocal) {
            System.out.println(" ::::::::  "+eachPath.getName().trim());
            if (eachPath.getName().trim().equals("Unmappedkey")) {
                loadMappingHashMap(new Path("/Users/ashish/IdeaProjects/KVSAggregationEncodingAvro/out1/Unmappedkey/-r-00000"));
                //loadMappingHashMap(new Path(eachPath.toString()+"/-r-00000.snappy"));
            }
        }
    }

    private void loadMappingHashMap(Path filePath) throws IOException {
        String strLineRead;
        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));
            while ((strLineRead = brReader.readLine()) != null) {
                String mappingValues[] = strLineRead.split("\t");
                if (mappingValues.length > 1) {
                    cache.put(mappingValues[1], mappingValues[0]);
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
            //String event_type = aggr.getEVENTTYPE().toString();
            String platform = aggr.getPLATFORM().toString();
            String os = aggr.getOSVERSION().toString();
            String application = aggr.getAPPLICATION().toString();
            String bundle_id = aggr.getBUNDLEID().toString();
            String kv_id = aggr.getKVID().toString();

            /*event_type = event_type != null ? event_type.equals("PUT") ? "1" : "0" : null;
            aggr.setEVENTTYPE(event_type);*/
            aggr.setPLATFORM(extractFromCache(platform));
            aggr.setOSVERSION(extractFromCache(os));
            aggr.setAPPLICATION(extractFromCache(application));
            aggr.setBUNDLEID(extractFromCache(bundle_id));
            aggr.setKVID(extractFromCache(kv_id));

            writeEncodedData(context, aggr);
        }
    }

    private void writeEncodedData(final Context context, final Avro_aggrDaily_KVSDlyAggr0 record) throws IOException, InterruptedException {
        mos.write(FULL_ENCODED_DATA, new AvroKey<Avro_aggrDaily_KVSDlyAggr0>(record), NullWritable.get(), context.getConfiguration().get(FULL_ENCODED_DATA));
    }

    private String extractFromCache(String field) {
        if (field != null) {
            String cacheField = cache.getIfPresent(field);
            if (cacheField != null && field.length() > cacheField.length()) {
                return cacheField;
            }
        }
        return field;
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        mos.close();
    }

}
