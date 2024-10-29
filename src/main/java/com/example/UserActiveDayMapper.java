package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;// 处理与输入和输出操作相关的错误

public class UserActiveDayMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分出每一列
        String[] fields = value.toString().split(",");

        // 排除第一行的标题
        if (fields[0].equals("user_id")) {
            return;
        }

        String userId = fields[0];
        String directPurchaseAmt = fields[5];
        String totalRedeemAmt = fields[8];
        long active = 0;

        // 处理空白值并判断用户是否活跃
        if ((!directPurchaseAmt.isEmpty() && !directPurchaseAmt.equals("0")) ||
                (!totalRedeemAmt.isEmpty() && !totalRedeemAmt.equals("0"))) {
            active = 1;
        }

        // 输出键值对
        context.write(new Text(userId), new LongWritable(active));
    }
}
