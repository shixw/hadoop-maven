package com.hadoop.mr.dc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xianwei on 2015/8/14.
 * 序列化实例  需要实现 org.apache.hadoop.io.Writable 接口
 */
public class DataBean implements Writable {
    private String tleNo;
    private long upPayLoad;
    private long downPayLoad;

    private long totalPayLoad;

    public DataBean() {
    }

    public DataBean(String tleNo, long upPayLoad, long downPayLoad) {
        this.tleNo = tleNo;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
        this.totalPayLoad = upPayLoad + downPayLoad;
    }

    //序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tleNo);
        dataOutput.writeLong(upPayLoad);
        dataOutput.writeLong(downPayLoad);
        dataOutput.writeLong(totalPayLoad);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.tleNo = dataInput.readUTF();
        this.upPayLoad = dataInput.readLong();
        this.downPayLoad = dataInput.readLong();
        this.totalPayLoad = dataInput.readLong();
    }

    @Override
    public String toString() {
        return this.upPayLoad + "\t" + this.downPayLoad + "\t" + "\t" + this.totalPayLoad;
    }

    public long getTotalPayLoad() {
        return totalPayLoad;
    }

    public void setTotalPayLoad(long totalPayLoad) {
        this.totalPayLoad = totalPayLoad;
    }

    public String getTleNo() {
        return tleNo;
    }

    public void setTleNo(String tleNo) {
        this.tleNo = tleNo;
    }

    public long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }
}
