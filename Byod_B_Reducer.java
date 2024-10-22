package ujjwal;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Byod_B_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<double[]> points = new ArrayList<>();
        int dimensions = 0;

        // Collect all points
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            dimensions = parts.length;
            double[] point = new double[dimensions];
            for (int i = 0; i < dimensions; i++) {
                point[i] = Double.parseDouble(parts[i]);
            }
            points.add(point);
        }

        // Calculate the new centroid
        double[] newCentroid = new double[dimensions];
        for (double[] point : points) {
            for (int i = 0; i < dimensions; i++) {
                newCentroid[i] += point[i];
            }
        }

        for (int i = 0; i < dimensions; i++) {
            newCentroid[i] /= points.size();
        }

        // Convert new centroid to a string and write it out
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < newCentroid.length; i++) {
            sb.append(newCentroid[i]);
            if (i < newCentroid.length - 1) {
                sb.append(",");
            }
        }

        context.write(key, new Text(sb.toString()));
    }
}