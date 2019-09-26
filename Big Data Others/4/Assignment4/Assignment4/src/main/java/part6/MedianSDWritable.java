package part6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianSDWritable implements Writable {
    private float standardDeviation_Ratings;
    private float median_Ratings;

    public MedianSDWritable() {
        super();
    }

    public MedianSDWritable(float standardDeviation_Ratings, float median_Ratings) {
        super();
        this.standardDeviation_Ratings = standardDeviation_Ratings;
        this.median_Ratings = median_Ratings;
    }

    public float getStandardDeviation_Ratings() {
        return standardDeviation_Ratings;
    }

    public void setStandardDeviation_Ratings(float standardDeviation_Ratings) {
        this.standardDeviation_Ratings = standardDeviation_Ratings;
    }

    public float getMedian_Ratings() {
        return median_Ratings;
    }

    public void setMedian_Ratings(float median_Ratings) {
        this.median_Ratings = median_Ratings;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(standardDeviation_Ratings);
        dataOutput.writeFloat(median_Ratings);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        standardDeviation_Ratings=dataInput.readFloat();
        median_Ratings=dataInput.readFloat();
    }
    @Override
    public String toString(){
        return "Standard Deviation : "+ standardDeviation_Ratings+" --- Median_Ratings" + median_Ratings;
    }
}
