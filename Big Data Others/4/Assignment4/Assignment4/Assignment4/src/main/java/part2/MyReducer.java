package part2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyReducer extends Reducer<Text,StockWritable,Text,StockWritable> {
    @Override
    protected void reduce(Text key, Iterable<StockWritable> values, Context context) throws IOException, InterruptedException {
        long maxVolume =0;
        long minVolume =0;
        float maxStockPriceAdj=0.0f;
        boolean flag = true;
        StockWritable swf = new StockWritable();

        for(StockWritable val : values){
            if(flag){
                maxVolume=val.getStockVolume();
                minVolume=val.getStockVolume();
                maxStockPriceAdj=val.getMaxStockPriceAdjClose();
                flag=false;
            }

            if(minVolume>val.getStockVolume()){
                minVolume=val.getStockVolume();
                swf.setMinStockVolumeDate(val.getMinStockVolumeDate());
            }

            if(maxVolume<val.getStockVolume()){
                maxVolume=val.getStockVolume();
                swf.setMaxStockVolumeDate(val.getMaxStockVolumeDate());
            }

            if(val.getMaxStockPriceAdjClose()>maxStockPriceAdj){
                maxStockPriceAdj=val.getMaxStockPriceAdjClose();
                swf.setMaxStockPriceAdjClose(val.getMaxStockPriceAdjClose());
            }
        }

        swf.setStockVolume(0);
        context.write(key,swf);
    }
}
