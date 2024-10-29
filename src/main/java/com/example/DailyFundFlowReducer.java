package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyFundFlowReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalInflow = 0;
        long totalOutflow = 0;
        for (Text value : values) {
            String[] inflowOutflow = value.toString().split(",");
            totalInflow += Long.parseLong(inflowOutflow[0]);
            totalOutflow += Long.parseLong(inflowOutflow[1]);
        }
        context.write(key, new Text(totalInflow + "," + totalOutflow));
    }

}
