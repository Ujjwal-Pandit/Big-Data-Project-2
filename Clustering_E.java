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

public class Clustering_E {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Clustering_E <input path> <output path> <max iterations> <convergence threshold>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        String outputPath = args[1];
        int maxIterations = Integer.parseInt(args[2]);
        double convergenceThreshold = Double.parseDouble(args[3]);

        String initialCentroidsPath = "/project2/Input_Files/Kseed3.csv";
        String currentCentroidsPath = initialCentroidsPath;
        List<double[]> previousCentroids = null;

        boolean converged = false;
        int iteration;
        for (iteration = 0; iteration < maxIterations && !converged; iteration++) {
            Job job = Job.getInstance(conf, "KMeans Clustering Iteration " + (iteration + 1));

            job.setJarByClass(Clustering_E.class);
            job.setMapperClass(Clustering_E_Mapper.class);
            job.setCombinerClass(Clustering_E_Combiner.class);
            job.setReducerClass(Clustering_E_Reducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.getConfiguration().set("centroids.path", currentCentroidsPath);
            job.getConfiguration().setBoolean("final.iteration", false);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            String iterationOutput = outputPath + "/iteration_" + iteration;
            FileOutputFormat.setOutputPath(job, new Path(iterationOutput));

            boolean success = job.waitForCompletion(true);
            if (!success) {
                System.err.println("Job failed. Exiting...");
                System.exit(1);
            }

            String newCentroidsPath = outputPath + "/centroids_" + (iteration + 1) + ".csv";
            List<double[]> newCentroids = updateCentroids(fs, iterationOutput + "/part-r-00000", newCentroidsPath);

            if (previousCentroids != null) {
                double maxChange = getMaxChange(previousCentroids, newCentroids);
                System.out.println("Iteration " + (iteration + 1) + " - Max centroid change: " + (maxChange * 100) + "%");

                if (maxChange <= convergenceThreshold) {
                    converged = true;
                    System.out.println("Converged after " + (iteration + 1) + " iterations.");
                    System.out.println("Final relative threshold: " + (maxChange * 100) + "%");
                }
            }

            previousCentroids = newCentroids;
            currentCentroidsPath = newCentroidsPath;
        }

        if (!converged) {
            System.out.println("Maximum iterations (" + maxIterations + ") reached without convergence.");
            System.out.println("Final threshold after " + iteration + " iterations: " +
                    (getMaxChange(previousCentroids, updateCentroids(fs, outputPath + "/iteration_" + (iteration-1) + "/part-r-00000",
                            outputPath + "/final_centroids.csv")) * 100) + "%");
        }

        // Output for e.i: Cluster centers and convergence indication
        writeClusterCenters(fs, currentCentroidsPath, outputPath + "/cluster_centers.txt", converged, iteration);

        // Output for e.ii: Final clustered data points with cluster centers
        runFinalIteration(conf, fs, inputPath, outputPath, currentCentroidsPath);
    }

    private static void writeClusterCenters(FileSystem fs, String centroidsPath, String outputPath, boolean converged, int iterations) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(centroidsPath))));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputPath))));

        if (converged) {
            bw.write("Convergence reached after " + iterations + " iterations.\n\n");
        } else {
            bw.write("Maximum iterations (" + iterations + ") reached without convergence.\n\n");
        }

        bw.write("Cluster Centers:\n");

        String line;
        int centroidIndex = 0;
        while ((line = br.readLine()) != null) {
            bw.write("Centroid " + centroidIndex + ": " + line + "\n");
            centroidIndex++;
        }

        br.close();
        bw.close();
    }

    private static void runFinalIteration(Configuration conf, FileSystem fs, String inputPath, String outputPath, String centroidsPath) throws Exception {
        Job job = Job.getInstance(conf, "KMeans Final Clustering");

        job.setJarByClass(Clustering_E.class);
        job.setMapperClass(Clustering_E_Mapper.class);
        job.setReducerClass(Clustering_E_Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("centroids.path", centroidsPath);
        job.getConfiguration().setBoolean("final.iteration", true);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/final_clustering"));

        job.waitForCompletion(true);
    }

    private static List<double[]> updateCentroids(FileSystem fs, String reducerOutputPath, String newCentroidsPath) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(reducerOutputPath))));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(newCentroidsPath))));

        List<double[]> newCentroids = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            bw.write(parts[1]);
            bw.newLine();

            String[] centroidParts = parts[1].split(",");
            double[] centroid = new double[centroidParts.length];
            for (int i = 0; i < centroidParts.length; i++) {
                centroid[i] = Double.parseDouble(centroidParts[i]);
            }
            newCentroids.add(centroid);
        }

        br.close();
        bw.close();
        return newCentroids;
    }

    private static double getMaxChange(List<double[]> previousCentroids, List<double[]> newCentroids) {
        double maxRelativeChange = 0.0;
        for (int i = 0; i < previousCentroids.size(); i++) {
            double[] prev = previousCentroids.get(i);
            double[] current = newCentroids.get(i);
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