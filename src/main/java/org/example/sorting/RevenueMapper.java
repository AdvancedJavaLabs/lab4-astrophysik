package org.example.sorting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RevenueMapper extends Mapper<Object, Text, DoubleWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length >= 3) {
            try {
                double revenue = Double.parseDouble(fields[1]); // Предполагаем, что выручка - второй элемент
                context.write(new DoubleWritable(revenue), value);
            } catch (NumberFormatException e) {
                // Логирование ошибок преобразования
            }
        }
    }
}