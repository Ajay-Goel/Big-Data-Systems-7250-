package part6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RatingWritable implements Writable {
    private float ratings;
    private long count;

    public RatingWritable() {
        super();
    }

    public RatingWritable(int ratings, long count) {
        super();
        this.ratings = ratings;
        this.count = count;
    }

    public float getRatings() {
        return ratings;
    }

    public void setRatings(int ratings) {
        this.ratings = ratings;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeFloat(ratings);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count=dataInput.readLong();
        ratings=dataInput.readFloat();
    }
    @Override
    public String toString(){
        return "Ratings: "+ ratings +" Count: "+count;
    }
}
