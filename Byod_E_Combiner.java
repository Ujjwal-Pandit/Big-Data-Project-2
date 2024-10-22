package org.ujjwal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Byod_E_Combiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<double[]> points = new ArrayList<>();
        int dimensions = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            dimensions = parts.length - 1;  // Last part is the count
            double[] point = new double[dimensions];
            for (int i = 0; i < dimensions; i++) {
                point[i] = Double.parseDouble(parts[i]);
            }
            points.add(point);
        }

        double[] sum = new double[dimensions];
        for (double[] point : points) {
            for (int i = 0; i < dimensions; i++) {
                sum[i] += point[i];
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sum.length; i++) {
            sb.append(sum[i]);
            if (i < sum.length - 1) {
                sb.append(",");
            }
        }
        sb.append("," + points.size());  // Append count of points

        context.write(key, new Text(sb.toString()));
    }
}