package part4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparator extends WritableComparator {
    protected SecondarySortComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyWritable ckw1 = (CompositeKeyWritable) a;
        CompositeKeyWritable ckw2 = (CompositeKeyWritable) b;
        int result = ckw1.getStockSymbol().compareTo(ckw2.getStockSymbol());
        if(result==0)
        {
            return -ckw1.getAccessDate().compareTo(ckw2.getAccessDate());
        }
        return result;
    }
}
