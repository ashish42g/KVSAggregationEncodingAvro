package com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

import static com.apple.irpt.df.jobs.kvs.dataWrite.utility.KVSUtils.*;

public class KVSReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private MultipleOutputs<Text, NullWritable> mos;
    private long rank = 0;

    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
/*      context.getCounter(MAP_COUNTER.ID_COUNTER).increment(1);
        String id = System.currentTimeMillis() / 1000 + "" + context.getTaskAttemptID().getTaskID().getId() + context.getCounter(MAP_COUNTER.ID_COUNTER).getValue();*/

        if (rank == 0) {
            rank = Long.parseLong(key.toString().split("\t")[1]) + 1;
        }
        else {
            rank++;
        }
        mos.write(UNMAPPED_KEY, new Text(rank + "\t" + key.toString().split("\t")[0]), NullWritable.get(), context.getConfiguration().get(UNMAPPED_KEY));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
