package part4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
    private String StockSymbol;
    private Date AccessDate;

    public CompositeKeyWritable() {
        super();
    }

    public CompositeKeyWritable(String stockSymbol, Date accessDate) {
        super();
        this.StockSymbol = stockSymbol;
        this.AccessDate = accessDate;
    }

    public String getStockSymbol() {
        return StockSymbol;
    }

    public void setStockSymbol(String stockSymbol) {
        StockSymbol = stockSymbol;
    }

    public Date getAccessDate() {
        return AccessDate;
    }

    public void setAccessDate(Date accessDate) {
        AccessDate = accessDate;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.StockSymbol);
        dataOutput.writeLong(this.AccessDate.getTime());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.StockSymbol=dataInput.readUTF();
        this.AccessDate= new Date(dataInput.readLong());
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int result = this.StockSymbol.compareTo(o.StockSymbol);
        if(result ==0){
            return this.AccessDate.compareTo(o.AccessDate);
        }
        return result;
    }

    @Override
    public String toString(){
        return "Stock Symbol "+this.StockSymbol+" , "+"Access Date"+this.AccessDate;
    }

}
