package com.lee.hadoop.mr.datacount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataBean implements Writable {

	private String mobile;
	
	private long upPayload;
	
	private long downPayload;
	
	private long totalPayload;
	
	public DataBean() {}

	public DataBean(String mobile, long upPayload, long downPayload) {
		super();
		this.mobile = mobile;
		this.upPayload = upPayload;
		this.downPayload = downPayload;
		this.totalPayload = upPayload + downPayload;
	}
	
	/**
	 * 序列化 成字节流
	 * 注意: 顺序, 类型
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(mobile);
		out.writeLong(upPayload);
		out.writeLong(downPayload);
		out.writeLong(totalPayload);
	}
	
	/**
	 * 反序列化 将字节流序列化成对象
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.mobile = in.readUTF();
		this.upPayload = in.readLong();
		this.downPayload = in.readLong();
		this.totalPayload = in.readLong();
	}
	
	/**
	 * 为了显示具体的值, 而不是hashcode值
	 */
	@Override
	public String toString() {
		return (this.upPayload + "\t" + this.downPayload + "\t" + this.totalPayload).toString();
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public long getUpPayload() {
		return upPayload;
	}

	public void setUpPayload(long upPayload) {
		this.upPayload = upPayload;
	}

	public long getDownPayload() {
		return downPayload;
	}

	public void setDownPayload(long downPayload) {
		this.downPayload = downPayload;
	}

	public long getTotalPayload() {
		return totalPayload;
	}

	public void setTotalPayload(long totalPayload) {
		this.totalPayload = totalPayload;
	}
}
