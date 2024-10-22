package org.ujjwal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class Medoids_B_Mapper extends Mapper<Object, Text, IntWritable, Text> {

    private ArrayList<double[]> medoids;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String medoidsPath = conf.get("medoids.path");
        Path medoidsFilePath = new Path(medoidsPath);

        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(medoidsFilePath)));

        String line;
        medoids = new ArrayList<>();

        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            double[] medoid = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                medoid[i] = Double.parseDouble(parts[i]);
            }
            medoids.add(medoid);
        }
        br.close();
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        double[] point = new double[parts.length];

        for (int i = 0; i < parts.length; i++) {
            point[i] = Double.parseDouble(parts[i]);
        }

        int nearestMedoid = 0;
        double nearestDistance = Double.MAX_VALUE;

        for (int i = 0; i < medoids.size(); i++) {
            double distance = calculateDistance(point, medoids.get(i));
            if (distance < nearestDistance) {
                nearestDistance = distance;
                nearestMedoid = i;
            }
        }

        context.write(new IntWritable(nearestMedoid), value);
    }

    private double calculateDistance(double[] point, double[] medoid) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += Math.pow(point[i] - medoid[i], 2);
        }
        return Math.sqrt(sum);
    }
}