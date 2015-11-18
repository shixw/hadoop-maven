package com.hadoop.mr.temperature;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xianwei on 2015/8/23.
 */
public class KeyPari implements WritableComparable<KeyPari> {
    private int year;
    private int hot;

    @Override
    public String toString() {
        return this.year + "\t" + this.hot;
    }

    @Override
    public int hashCode() {
        return new Integer(year + hot).hashCode();
    }

    //返回 负整数、零或正整数，根据此对象是小于、等于还是大于指定对象。
    @Override
    public int compareTo(KeyPari o) {
        int year_result = Integer.compare(this.year, o.getYear());
        if (year_result != 0) {//表示年份不相等
            return year_result;
        }
        //相等
        return Integer.compare(this.hot, o.getHot());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.year);
        out.writeInt(this.hot);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.hot = in.readInt();
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }


}
