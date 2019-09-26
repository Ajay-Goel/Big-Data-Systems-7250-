package part2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StockWritable implements Writable {

    private Date maxStockVolumeDate;
    private Date minStockVolumeDate;
    private long StockVolume;
    private float maxStockPriceAdjClose;

    public StockWritable() {
        super();
    }

    public StockWritable(Date minStockVolumeDate, Date maxStockVolumeDate, long stockVolume, float maxStockPriceAdjClose) {
        super();
        this.maxStockPriceAdjClose=maxStockPriceAdjClose;
        this.minStockVolumeDate=minStockVolumeDate;
        this.StockVolume=stockVolume;
        this.maxStockVolumeDate=maxStockVolumeDate;
    }


    public Date getMaxStockVolumeDate() {
        return maxStockVolumeDate;
    }

    public void setMaxStockVolumeDate(Date maxStockVolumeDate) {
        this.maxStockVolumeDate = maxStockVolumeDate;
    }

    public Date getMinStockVolumeDate() {
        return minStockVolumeDate;
    }

    public void setMinStockVolumeDate(Date minStockVolumeDate) {
        this.minStockVolumeDate = minStockVolumeDate;
    }

    public long getStockVolume() {
        return StockVolume;
    }

    public void setStockVolume(long stockVolume) {
        StockVolume = stockVolume;
    }

    public float getMaxStockPriceAdjClose() {
        return maxStockPriceAdjClose;
    }

    public void setMaxStockPriceAdjClose(float maxStockPriceAdjClose) {
        this.maxStockPriceAdjClose = maxStockPriceAdjClose;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.maxStockVolumeDate.getTime());
        dataOutput.writeLong(this.minStockVolumeDate.getTime());
        dataOutput.writeFloat(this.maxStockPriceAdjClose);
        dataOutput.writeLong(this.StockVolume);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.maxStockPriceAdjClose=dataInput.readFloat();
        this.maxStockVolumeDate= new Date(dataInput.readLong());
        this.minStockVolumeDate= new Date(dataInput.readLong());
        this.StockVolume = dataInput.readLong();
    }

    @Override
    public String toString(){
        return "MinStockVolumeDate= "+minStockVolumeDate +" maxStockVolumeDate= "+maxStockVolumeDate+" maxStockPriceAdjClose= "+maxStockPriceAdjClose+" stockVolume= "+StockVolume;
    }
}
