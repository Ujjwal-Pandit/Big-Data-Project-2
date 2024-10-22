package ujjwal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class Byod_D {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: Byod_D <input path> <output path> <max iterations> <convergence threshold>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        String inputPath = args[0];
        String outputPath = args[1];
        int maxIterations = Integer.parseInt(args[2]);
        double convergenceThreshold = Double.parseDouble(args[3]);

        String initialCentroidsPath = "/project2/Input_Files/College_Kseed.csv";
        String currentCentroidsPath = initialCentroidsPath;
        List<double[]> previousCentroids = null;

        boolean converged = false;
        int iteration;
        for (iteration = 0; iteration < maxIterations && !converged; iteration++) {
            Job job = Job.getInstance(conf, "KMeans Clustering Iteration " + (iteration + 1));

            job.setJarByClass(Byod_D.class);
            job.setMapperClass(Byod_D_Mapper.class);
            job.setCombinerClass(Byod_D_Combiner.class);  // Add this line
            job.setReducerClass(Byod_D_Reducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            job.getConfiguration().set("centroids.path", currentCentroidsPath);

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