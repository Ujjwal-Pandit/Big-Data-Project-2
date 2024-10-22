package org.ujjwal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Byod_E_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private boolean isFinalIteration;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        isFinalIteration = context.getConfiguration().getBoolean("final.iteration", false);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        if (isFinalIteration) {
            for (Text value : values) {
                context.write(key, value);
            }
        } else {
            double[] sum = null;
            int totalCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (sum == null) {
                    sum = new double[parts.length - 1];
                }
                for (int i = 0; i < parts.length - 1; i++) {
                    sum[i] += Double.parseDouble(parts[i]);
                }
                totalCount += Integer.parseInt(parts[parts.length - 1]);
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < sum.length; i++) {
                sum[i] /= totalCount;
                sb.append(sum[i]);
                if (i < sum.length - 1) {
                    sb.append(",");
                }
            }

            context.write(key, new Text(sb.toString()));
        }
    }
}