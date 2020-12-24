package com.tiza.leo.mobileaccesslogadvance;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:17
 * Content: 为达到统计某个手机上行流量 下行流量  总流量的目的
 *  map 输入 key    行首在整个源数据中的偏移量
 *          value  源数据中的一行文本
 *      输出 key    一行文本按照\t分割后的第2个值
 *          value   一行文本按照\t分割后的第7,8,(7+8)个值  也就是自定义类AccessLog4Writable
 *   shuffle   自动处理的排序及分组  注意会按照map输出的key 进行分组，所以给reduce的内容为 key,[value1,value2,...]
 *   reduce中的输入   key   同map 输出key
 *                   value map 输出值 的 数组形式  --> 所以可以合并 上 下 行流量
 */
public class AccessLog4Writable implements WritableComparable<AccessLog4Writable> {
    // 用此类封装map , reduce 的value 值 （key 是手机号码）
    private Integer upload;
    private Integer down;
    private Integer total;

    public int compareTo(AccessLog4Writable o) {
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
        AccessLog4Writable that = (AccessLog4Writable) o;
        return Objects.equals(upload, that.upload) &&
                Objects.equals(down, that.down) &&
                Objects.equals(total, that.total);
    }

    @Override
    public int hashCode() {

        return Objects.hash(upload, down, total);
    }

    public AccessLog4Writable() {
    }

    public AccessLog4Writable(Integer upload, Integer down) {
        this.upload = upload;
        this.down = down;
    }

    public AccessLog4Writable(Integer upload, Integer down, Integer total) {
        this.upload = upload;
        this.down = down;
        this.total = total;
    }

   /* @Override
    public String toString() {
        return "统计结果{" +
                "上传流量=" + upload +
                ", 下载流量=" + down +
                ",  上传下载总流量=" + total +
                '}';
    }*/

   public String toString(){
       return upload + "\t"+ down +"\t" + total;
   }
}
