package com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVSCombiner extends Reducer<Text, NullWritable, Text, NullWritable>{

    @Override
    protected void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException,InterruptedException {
        context.write(key, NullWritable.get());
    }

}
