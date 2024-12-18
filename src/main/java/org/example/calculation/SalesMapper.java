package org.example.calculation;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields[0].equals("transaction_id") || fields.length != 5) {
            return;
        }

        try {
            String category = fields[2];
            double price = Double.parseDouble(fields[3]);
            int quantity = Integer.parseInt(fields[4]);
            double revenue = price * quantity;

            context.write(new Text(category), new SalesWritable(revenue, quantity));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }
}