package org.ujjwal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class Clustering_B {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Clustering_B <input path> <output path> <number of iterations>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        String outputPath = args[1];
        int numIterations = Integer.parseInt(args[2]);

        // Initial centroids path
        String initialCentroidsPath = "/project2/Input_Files/Kseed.csv";
        String currentCentroidsPath = initialCentroidsPath;

        for (int i = 0; i < numIterations; i++) {
            Job job = Job.getInstance(conf, "KMeans Clustering Iteration " + (i + 1));

            job.setJarByClass(Clustering_B.class);
            job.setMapperClass(Clustering_B_Mapper.class);
            job.setReducerClass(Clustering_B_Reducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // Set the current centroids path in the job configuration
            job.getConfiguration().set("centroids.path", currentCentroidsPath);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            String iterationOutput = outputPath + "/iteration_" + i;
            FileOutputFormat.setOutputPath(job, new Path(iterationOutput));

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.err.println("Job failed. Exiting...");
                System.exit(1);
            }

            // Update centroids for next iteration
            String newCentroidsPath = outputPath + "/centroids_" + (i + 1) + ".csv";
            updateCentroids(fs, iterationOutput + "/part-r-00000", newCentroidsPath);
            currentCentroidsPath = newCentroidsPath;
        }
    }

    private static void updateCentroids(FileSystem fs, String reducerOutputPath, String newCentroidsPath) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(reducerOutputPath))));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(newCentroidsPath))));

        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            bw.write(parts[1]);
            bw.newLine();
        }

        br.close();
        bw.close();
    }
}