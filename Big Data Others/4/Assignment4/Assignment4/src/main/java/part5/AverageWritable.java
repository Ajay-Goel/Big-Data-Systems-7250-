package part5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageWritable implements Writable {
    private double average;
    private long count;

    public AverageWritable() {
        super();
    }

    public AverageWritable(double average, int count){
        super();
        this.average=average;
        this.count=count;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(this.average);
        dataOutput.writeLong(this.count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count=dataInput.readLong();
        this.average=dataInput.readDouble();
    }

    @Override
    public String toString(){
        return "Average: "+average+" Count: "+ count;
    }
}
