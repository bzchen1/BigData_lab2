package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeekFundFlowReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalInflow = 0;
        long totalOutflow = 0;
        long day_num = 0;
        for (Text value : values) {
            String[] inflowOutflow = value.toString().split(",");
            totalInflow += Long.parseLong(inflowOutflow[0]);
            totalOutflow += Long.parseLong(inflowOutflow[1]);
            day_num = day_num + 1;
        }
        context.write(key, new Text(totalInflow / day_num + "," + totalOutflow / day_num));
    }
}
