package org.ujjwal;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Medoids_D_Combiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> points = new ArrayList<>();

        for (Text val : values) {
            points.add(val.toString());
        }

        // Output all points for this medoid
        for (String point : points) {
            context.write(key, new Text(point));
        }
    }
}