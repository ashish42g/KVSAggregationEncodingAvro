package com.apple.irpt.df.jobs.kvs.dataWrite;

import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSCombiner;
import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSMapper1;
import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSMapper2;
import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.KVSReducer;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.apple.irpt.df.jobs.kvs.dataWrite.schema.aggrDaily.Avro_aggrDaily_KVSDlyAggr0.SCHEMA$;
import static com.apple.irpt.df.jobs.kvs.dataWrite.utility.KVSUtils.*;

public class Driver extends Configured implements Tool {

    private static final Logger LOGGER = Logger.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        if (args.length < 3) {
            LOGGER.error("This job expects four command line arguments: \n" +
                    "args[0] is input file path, \n" +
                    "args[1] is mapping file path, \n" +
                    "args[2] intermediate output file path \n" +
                    "and args[3] is final output path");
            return 1;
        }

        LOGGER.info("Job Input: " + args[0]);
        LOGGER.info("Job Output:" + args[1]);

        //initializing First Job
        Configuration config = getConf();
        Job job1 = new Job(config, getClass().getName());

        //Configurations
        DistributedCache.addCacheFile(new URI(args[1]), job1.getConfiguration());

        job1.getConfiguration().set(INITIAL_ENCODED_DATA, args[2] + "/" + INITIAL_ENCODED_DATA + "/");
        job1.getConfiguration().set(PARTIAL_ENCODED_DATA, args[2] + "/" + PARTIAL_ENCODED_DATA + "/");
        job1.getConfiguration().set(UNMAPPED_KEY, args[2] + "/" + UNMAPPED_KEY + "/");

        MultipleOutputs.addNamedOutput(job1, UNMAPPED_KEY, TextOutputFormat.class, Text.class, NullWritable.class);

        AvroMultipleOutputs.addNamedOutput(job1, PARTIAL_ENCODED_DATA, AvroKeyOutputFormat.class, SCHEMA$);
        AvroMultipleOutputs.addNamedOutput(job1, INITIAL_ENCODED_DATA, AvroKeyOutputFormat.class, SCHEMA$);

        job1.setInputFormatClass(AvroKeyInputFormat.class);

        //Input Output Paths
        FileInputFormat.addInputPaths(job1, args[0]);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        //Schema
        AvroJob.setInputKeySchema(job1, SCHEMA$);
        AvroJob.setOutputKeySchema(job1, SCHEMA$);

        //Class Names
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(KVSMapper1.class);
        job1.setCombinerClass(KVSCombiner.class);
        job1.setReducerClass(KVSReducer.class);
        job1.setNumReduceTasks(1);

        //Input and output key and value
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        //O/P of reducer
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        //Snappy compression enable
        /*FileOutputFormat.setCompressOutput(job1, true);
        FileOutputFormat.setOutputCompressorClass(job1, SnappyCodec.class);*/

        if (job1.waitForCompletion(true)) {

            //initializing Second Job
            Job job2 = new Job(config, getClass().getName());

            //configuration
            DistributedCache.addCacheFile(new URI(args[2] + "/" + UNMAPPED_KEY + "/"), job2.getConfiguration());
            job2.getConfiguration().set(FULL_ENCODED_DATA, args[2] + "/" + FULL_ENCODED_DATA + "/");

            AvroMultipleOutputs.addNamedOutput(job2, FULL_ENCODED_DATA, AvroKeyOutputFormat.class, SCHEMA$);

            //Input and Output paths and format
            FileInputFormat.addInputPaths(job2, args[2] + "/" + PARTIAL_ENCODED_DATA + "/");
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            job2.setInputFormatClass(AvroKeyInputFormat.class);
            job2.setOutputFormatClass(AvroKeyOutputFormat.class);

            //Schema
            AvroJob.setInputKeySchema(job2, SCHEMA$);
            AvroJob.setMapOutputKeySchema(job2, SCHEMA$);
            AvroJob.setOutputKeySchema(job2, SCHEMA$);

            //Class Names
            job2.setJarByClass(Driver.class);
            job2.setMapperClass(KVSMapper2.class);

            job2.setMapOutputKeyClass(AvroKey.class);
            job2.setMapOutputValueClass(NullWritable.class);

            //Snappy Compression codec
            /*FileOutputFormat.setCompressOutput(job2, true);
            FileOutputFormat.setOutputCompressorClass(job2, SnappyCodec.class);*/

            job2.setNumReduceTasks(0);

            job2.waitForCompletion(true);
        }

        return 0;
    }
}
