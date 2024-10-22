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

public class Medoids_B {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Medoids_B <input path> <output path> <number of iterations>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        String outputPath = args[1];
        int numIterations = Integer.parseInt(args[2]);

        // Initial medoids path
        String initialMedoidsPath = "/project2/Input_Files/Kmedseed.csv";
        String currentMedoidsPath = initialMedoidsPath;

        for (int i = 0; i < numIterations; i++) {
            Job job = Job.getInstance(conf, "KMedoids Clustering Iteration " + (i + 1));

            job.setJarByClass(Medoids_B.class);
            job.setMapperClass(Medoids_B_Mapper.class);
            job.setReducerClass(Medoids_B_Reducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // Set the current medoids path in the job configuration
            job.getConfiguration().set("medoids.path", currentMedoidsPath);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            String iterationOutput = outputPath + "/iteration_" + i;
            FileOutputFormat.setOutputPath(job, new Path(iterationOutput));

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.err.println("Job failed. Exiting...");
                System.exit(1);
            }

            // Update medoids for next iteration
            String newMedoidsPath = outputPath + "/medoids_" + (i + 1) + ".csv";
            updateMedoids(fs, iterationOutput + "/part-r-00000", newMedoidsPath);
            currentMedoidsPath = newMedoidsPath;
        }
    }

    private static void updateMedoids(FileSystem fs, String reducerOutputPath, String newMedoidsPath) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(reducerOutputPath))));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(newMedoidsPath))));

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