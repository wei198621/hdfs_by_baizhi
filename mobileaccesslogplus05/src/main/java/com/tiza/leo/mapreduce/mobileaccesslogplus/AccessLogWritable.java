package com.tiza.leo.mapreduce.mobileaccesslogplus;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:17
 * Content:
 */
public class AccessLogWritable implements WritableComparable<AccessLogWritable> {

    private Integer upload;
    private Integer down;
    private Integer total;

    public int compareTo(AccessLogWritable o) {
        return this.total-o.getTotal();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(upload);
        out.writeInt(down);
        out.writeInt(total);
    }

    public void readFields(DataInput in) throws IOException {
        this.upload = in.readInt();
        this.down = in.readInt();
        this.total = in.readInt();
    }

    public Integer getUpload() {
        return upload;
    }

    public void setUpload(Integer upload) {
        this.upload = upload;
    }

    public Integer getDown() {
        return down;
    }

    public void setDown(Integer down) {
        this.down = down;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccessLogWritable that = (AccessLogWritable) o;
        return Objects.equals(upload, that.upload) &&
                Objects.equals(down, that.down) &&
                Objects.equals(total, that.total);
    }

    @Override
    public int hashCode() {

        return Objects.hash(upload, down, total);
    }

    public AccessLogWritable() {
    }

    public AccessLogWritable(Integer upload, Integer down) {
        this.upload = upload;
        this.down = down;
    }

    public AccessLogWritable(Integer upload, Integer down, Integer total) {
        this.upload = upload;
        this.down = down;
        this.total = total;
    }

    @Override
    public String toString() {
        return "统计结果{" +
                "上传流量=" + upload +
                ", 下载流量=" + down +
                ",  上传下载总流量=" + total +
                '}';
    }
}
