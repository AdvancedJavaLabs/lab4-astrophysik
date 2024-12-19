package org.example.calculation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, SalesWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<SalesWritable> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0;
        int totalQuantity = 0;

        for (SalesWritable val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }

        String result = String.format("%.2f\t%d", totalRevenue, totalQuantity);
        context.write(key, new Text(result));
    }
}