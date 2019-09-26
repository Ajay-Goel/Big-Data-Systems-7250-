package part3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, Text,Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long maxStockVolume=0;
        long minStockVolume=0;
        float maxStockPriceAdjClose=0.0f;
        //max stock_volume,  min stock_volume, max stock_price_adj_close
        for(Text val:values) {
            String Tokens[] = val.toString().split(",");
            if (maxStockVolume < Long.parseLong(Tokens[0]))
                maxStockVolume = Long.parseLong(Tokens[0]);

            if(minStockVolume > Long.parseLong(Tokens[1]))
                minStockVolume = Long.parseLong(Tokens[1]);

            if(maxStockPriceAdjClose < Float.parseFloat(Tokens[2]))
                maxStockPriceAdjClose = Float.parseFloat(Tokens[2]);
        }
        Text text = new Text(String.valueOf(maxStockVolume)+","+String.valueOf(minStockVolume)+","+String.valueOf(maxStockPriceAdjClose));
        context.write(key,text);
    }
}
