package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingLongWritableComparator extends WritableComparator {

    protected DescendingLongWritableComparator() {
        super(LongWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        LongWritable lw1 = (LongWritable) a;
        LongWritable lw2 = (LongWritable) b;
        return -lw1.compareTo(lw2); // 反转比较结果以实现降序
    }
}
