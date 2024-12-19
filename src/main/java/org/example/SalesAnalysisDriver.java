package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.calculation.SalesMapper;
import org.example.calculation.SalesReducer;
import org.example.calculation.SalesWritable;
import org.example.sorting.RevenueMapper;
import org.example.sorting.RevenueReducer;

import java.io.IOException;

public class SalesAnalysisDriver {

    public static boolean runCalculation(String input, String output, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job CountJob = Job.getInstance(conf, "Sales Analysis");

        CountJob.setJarByClass(SalesAnalysisDriver.class);
        CountJob.setMapperClass(SalesMapper.class);
        CountJob.setReducerClass(SalesReducer.class);

        CountJob.setMapOutputKeyClass(Text.class);
        CountJob.setMapOutputValueClass(SalesWritable.class);

        CountJob.setOutputKeyClass(Text.class);
        CountJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(CountJob, new Path(input));
        FileOutputFormat.setOutputPath(CountJob, new Path(output));
        return CountJob.waitForCompletion(true);
    }

    public static boolean runSort(String input, String output, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job sortJob = Job.getInstance(conf, "Sort by Revenue");
        sortJob.setJarByClass(SalesAnalysisDriver.class);
        sortJob.setMapperClass(RevenueMapper.class);
        sortJob.setReducerClass(RevenueReducer.class);

        sortJob.setMapOutputKeyClass(DoubleWritable.class);
        sortJob.setMapOutputValueClass(Text.class);

        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setSortComparatorClass(DoubleWritable.Comparator.class);

        FileInputFormat.addInputPath(sortJob, new Path(input));
        FileOutputFormat.setOutputPath(sortJob, new Path(output));
        return sortJob.waitForCompletion(true);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: SalesAnalysisDriver <input path> <tmp path> <output path>");
            System.exit(-1);
        }

        try {
            Configuration conf = new Configuration();
            boolean calculationResult = runCalculation(args[0], args[1], conf);
            if (!calculationResult) {
                System.exit(1);
            }
            boolean sortResult = runSort(args[1], args[2], conf);
            if (!sortResult) {
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}