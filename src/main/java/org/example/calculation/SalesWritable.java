package org.example.calculation;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalesWritable implements Writable {
    private double revenue;
    private int quantity;

    public SalesWritable() {}

    public SalesWritable(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    public double getRevenue() {
        return revenue;
    }

    public int getQuantity() {
        return quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readInt();
    }
}