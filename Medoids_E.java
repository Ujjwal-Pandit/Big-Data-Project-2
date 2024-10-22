package org.ujjwal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Medoids_E {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Medoids_E <input path> <output path> <max iterations> <convergence threshold>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        String outputPath = args[1];
        int maxIterations = Integer.parseInt(args[2]);
        double convergenceThreshold = Double.parseDouble(args[3]);

        String initialMedoidsPath = "/project2/Input_Files/Kmedseed3.csv";
        String currentMedoidsPath = initialMedoidsPath;
        List<double[]> previousMedoids = null;

        boolean converged = false;
        int iteration;
        for (iteration = 0; iteration < maxIterations && !converged; iteration++) {
            Job job = Job.getInstance(conf, "KMedoids Clustering Iteration " + (iteration + 1));

            job.setJarByClass(Medoids_E.class);
            job.setMapperClass(Medoids_E_Mapper.class);
            job.setCombinerClass(Medoids_E_Combiner.class);
            job.setReducerClass(Medoids_E_Reducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.getConfiguration().set("medoids.path", currentMedoidsPath);
            job.getConfiguration().setBoolean("final.iteration", false);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            String iterationOutput = outputPath + "/iteration_" + iteration;
            FileOutputFormat.setOutputPath(job, new Path(iterationOutput));

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.err.println("Job failed. Exiting...");
                System.exit(1);
            }

            String newMedoidsPath = outputPath + "/medoids_" + (iteration + 1) + ".csv";
            List<double[]> newMedoids = updateMedoids(fs, iterationOutput + "/part-r-00000", newMedoidsPath);

            if (previousMedoids != null) {
                double maxChange = getMaxChange(previousMedoids, newMedoids);
                System.out.println("Iteration " + (iteration + 1) + " - Max medoid change: " + (maxChange * 100) + "%");

                if (maxChange <= convergenceThreshold) {
                    converged = true;
                    System.out.println("Converged after " + (iteration + 1) + " iterations.");
                    System.out.println("Final relative threshold: " + (maxChange * 100) + "%");
                }
            }

            previousMedoids = newMedoids;
            currentMedoidsPath = newMedoidsPath;
        }

        if (!converged) {
            System.out.println("Maximum iterations (" + maxIterations + ") reached without convergence.");
            System.out.println("Final threshold after " + iteration + " iterations: " +
                    (getMaxChange(previousMedoids, updateMedoids(fs, outputPath + "/iteration_" + (iteration-1) + "/part-r-00000",
                            outputPath + "/final_medoids.csv")) * 100) + "%");
        }

        // Output for e.i: Cluster medoids and convergence indication
        writeMedoidClusters(fs, currentMedoidsPath, outputPath + "/cluster_medoids.txt", converged, iteration);

        // Output for e.ii: Final clustered data points with cluster medoids
        runFinalIteration(conf, fs, inputPath, outputPath, currentMedoidsPath);
    }

    private static void writeMedoidClusters(FileSystem fs, String medoidsPath, String outputPath, boolean converged, int iterations) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(medoidsPath))));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputPath))));

        if (converged) {
            bw.write("Convergence reached after " + iterations + " iterations.\n\n");
        } else {
            bw.write("Maximum iterations (" + iterations + ") reached without convergence.\n\n");
        }

        bw.write("Cluster Medoids:\n");

        String line;
        int medoidIndex = 0;
        while ((line = br.readLine()) != null) {
            bw.write("Medoid " + medoidIndex + ": " + line + "\n");
            medoidIndex++;
        }

        br.close();
        bw.close();
    }

    private static void runFinalIteration(Configuration conf, FileSystem fs, String inputPath, String outputPath, String medoidsPath) throws Exception {
        Job job = Job.getInstance(conf, "KMedoids Final Clustering");

        job.setJarByClass(Medoids_E.class);
        job.setMapperClass(Medoids_E_Mapper.class);
        job.setReducerClass(Medoids_E_Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("medoids.path", medoidsPath);
        job.getConfiguration().setBoolean("final.iteration", true);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/final_clustering"));

        job.waitForCompletion(true);
    }

    private static List<double[]> updateMedoids(FileSystem fs, String reducerOutputPath, String newMedoidsPath) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(reducerOutputPath))));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(newMedoidsPath))));

        List<double[]> newMedoids = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            bw.write(parts[1]);
            bw.newLine();

            String[] medoidParts = parts[1].split(",");
            double[] medoid = new double[medoidParts.length];
            for (int i = 0; i < medoidParts.length; i++) {
                medoid[i] = Double.parseDouble(medoidParts[i]);
            }
            newMedoids.add(medoid);
        }

        br.close();
        bw.close();
        return newMedoids;
    }

    private static double getMaxChange(List<double[]> previousMedoids, List<double[]> newMedoids) {
        double maxRelativeChange = 0.0;
        for (int i = 0; i < previousMedoids.size(); i++) {
            double[] prev = previousMedoids.get(i);
            double[] current = newMedoids.get(i);
            for (int j = 0; j < prev.length; j++) {
                if (prev[j] != 0) {  // Avoid division by zero
                    double relativeChange = Math.abs((current[j] - prev[j]) / prev[j]);
                    maxRelativeChange = Math.max(maxRelativeChange, relativeChange);
                }
            }
        }
        return maxRelativeChange;
    }
}