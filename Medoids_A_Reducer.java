package org.ujjwal;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Medoids_A_Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {

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

        // Find the new medoid (point with minimum total distance to all other points)
        double[] newMedoid = null;
        double minTotalDistance = Double.MAX_VALUE;

        for (double[] candidateMedoid : points) {
            double totalDistance = 0;
            for (double[] point : points) {
                totalDistance += calculateDistance(candidateMedoid, point);
            }
            if (totalDistance < minTotalDistance) {
                minTotalDistance = totalDistance;
                newMedoid = candidateMedoid;
            }
        }

        // Convert new medoid to a string and write it out
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < newMedoid.length; i++) {
            sb.append(newMedoid[i]);
            if (i < newMedoid.length - 1) {
                sb.append(",");
            }
        }

        context.write(key, new Text(sb.toString()));
    }

    private double calculateDistance(double[] point1, double[] point2) {
        double sum = 0;
        for (int i = 0; i < point1.length; i++) {
            sum += Math.pow(point1[i] - point2[i], 2);
        }
        return Math.sqrt(sum);
    }
}