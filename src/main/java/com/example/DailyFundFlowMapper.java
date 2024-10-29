package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;// 处理与输入和输出操作相关的错误

public class DailyFundFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分出每一列
        String[] fields = value.toString().split(",");

        // 排除第一列
        if (fields[0].equals("user_id"))
            return;
        String reportDate = fields[1];
        String totalPurchaseAmt = fields[4];
        String totalRedeemAmt = fields[8];

        // 处理空白值
        if (totalPurchaseAmt.isEmpty())
            totalPurchaseAmt = "0";
        if (totalRedeemAmt.isEmpty())
            totalRedeemAmt = "0";
        context.write(new Text(reportDate), new Text(totalPurchaseAmt + "," + totalRedeemAmt));
    }
}
