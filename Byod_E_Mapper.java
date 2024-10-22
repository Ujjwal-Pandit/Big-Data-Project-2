package org.ujjwal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Byod_E_Mapper extends Mapper<Object, Text, IntWritable, Text> {

    private ArrayList<double[]> centroids;
    private boolean isFinalIteration;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String centroidsPath = conf.get("centroids.path");
        Path centroidsFilePath = new Path(centroidsPath);

        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsFilePath)));

        String line;
        centroids = new ArrayList<>();

        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            double[] centroid = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                centroid[i] = Double.parseDouble(parts[i]);
            }
            centroids.add(centroid);
        }
        br.close();

        isFinalIteration = conf.getBoolean("final.iteration", false);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        double[] point = new double[parts.length];

        for (int i = 0; i < parts.length; i++) {
            point[i] = Double.parseDouble(parts[i]);
        }

        int nearestCentroid = 0;
        double nearestDistance = Double.MAX_VALUE;

        for (int i = 0; i < centroids.size(); i++) {
            double distance = calculateDistance(point, centroids.get(i));
            if (distance < nearestDistance) {
                nearestDistance = distance;
                nearestCentroid = i;
            }
        }

        if (isFinalIteration) {
            context.write(new IntWritable(nearestCentroid), value);
        } else {
            context.write(new IntWritable(nearestCentroid), new Text(value.toString() + ",1"));
        }
    }

    private double calculateDistance(double[] point, double[] centroid) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += Math.pow(point[i] - centroid[i], 2);
        }
        return Math.sqrt(sum);
    }
}