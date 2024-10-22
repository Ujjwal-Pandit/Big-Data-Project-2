package ujjwal;

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

public class Byod_A_Mapper extends Mapper<Object, Text, IntWritable, Text> {

    private ArrayList<double[]> centroids;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path centroidsPath = new Path("/project2/Input_Files/College_Kseed.csv");

        // Access HDFS
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));

        String line;
        centroids = new ArrayList<>();

        // Read centroids from Kseed.csv
        while ((line = br.readLine()) != null) {
            String[] parts = line.split(",");
            double[] centroid = new double[parts.length];
            for (int i = 0; i < parts.length; i++) {
                centroid[i] = Double.parseDouble(parts[i]);
            }
            centroids.add(centroid);
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

        int nearestCentroid = 0;
        double nearestDistance = Double.MAX_VALUE;

        for (int i = 0; i < centroids.size(); i++) {
            double distance = calculateDistance(point, centroids.get(i));
            if (distance < nearestDistance) {
                nearestDistance = distance;
                nearestCentroid = i;
            }
        }

        context.write(new IntWritable(nearestCentroid), value);
    }

    private double calculateDistance(double[] point, double[] centroid) {
        double sum = 0;
        for (int i = 0; i < point.length; i++) {
            sum += Math.pow(point[i] - centroid[i], 2);
        }
        return Math.sqrt(sum);
    }
}