package part4;

import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    protected GroupComparator() {
        super(CompositeKeyWritable.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        CompositeKeyWritable cwk1 = (CompositeKeyWritable) a;
        CompositeKeyWritable cwk2 = (CompositeKeyWritable) b;

        return cwk1.getStockSymbol().compareTo(cwk2.getStockSymbol());

    }
}
