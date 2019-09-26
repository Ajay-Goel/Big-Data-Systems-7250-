package part7;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianSDWritable implements Writable {
    private double standardDeviation_Ratings;
    private double median_Ratings;

    public MedianSDWritable() {
        super();
    }

    public MedianSDWritable(double standardDeviation_Ratings, double median_Ratings) {
        super();
        this.standardDeviation_Ratings = standardDeviation_Ratings;
        this.median_Ratings = median_Ratings;
    }

    public double getStandardDeviation_Ratings() {
        return standardDeviation_Ratings;
    }

    public void setStandardDeviation_Ratings(double standardDeviation_Ratings) {
        this.standardDeviation_Ratings = standardDeviation_Ratings;
    }

    public double getMedian_Ratings() {
        return median_Ratings;
    }

    public void setMedian_Ratings(double median_Ratings) {
        this.median_Ratings = median_Ratings;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(standardDeviation_Ratings);
        dataOutput.writeDouble(median_Ratings);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        standardDeviation_Ratings=dataInput.readDouble();
        median_Ratings=dataInput.readDouble();
    }
    @Override
    public String toString(){
        return "Standard Deviation : "+ standardDeviation_Ratings+" --- Median_Ratings" + median_Ratings;
    }
}
