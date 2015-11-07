package com.lee.hadoop.mr.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class InfoBean implements WritableComparable<InfoBean> {

	private String account;				// 账号
	private double income;			// 收入
	private double expenses;		// 支出
	private double surplus;			// 结余
	
//	public InfoBean() {}
//
//	public InfoBean(String account, BigDecimal income, BigDecimal expenses) {
//		super();
//		this.account = account;
//		this.income = income;
//		this.expenses = expenses;
//		this.surplus = income.subtract(expenses);
//	}
	
	public void set(String account, double income, double expenses) {
		this.account = account;
		this.income = income;
		this.expenses = expenses;
		this.surplus = income - expenses;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeDouble(income);
		out.writeDouble(expenses);
		out.writeDouble(surplus);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.account = in.readUTF();
		this.income = in.readDouble();
		this.expenses = in.readDouble();
		this.surplus = in.readDouble();
	}

	@Override
	public String toString() {
		// 在spring中可作为单例
//		NumberFormat currency = NumberFormat.getCurrencyInstance(); //建立货币格式化引用 
//	    NumberFormat percent = NumberFormat.getPercentInstance();  //建立百分比格式化引用 
//	    percent.setMaximumFractionDigits(3); //百分比小数点最多3位 
	    
//		return currency.format(income) + "\t" + currency.format(expenses) + "\t" + currency.format(surplus);
		return this.income + "\t" + this.expenses + "\t" + this.surplus;
	}

	// 返回1就是从小到大排序
	@Override
	public int compareTo(InfoBean o) {
		if (this.income == o.getIncome()) {
			return this.expenses > o.getExpenses() ? 1 : -1;
		} else {
			return this.income > o.getIncome() ? -1 : 1;
		}
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public double getIncome() {
		return income;
	}

	public void setIncome(double income) {
		this.income = income;
	}

	public double getExpenses() {
		return expenses;
	}

	public void setExpenses(double expenses) {
		this.expenses = expenses;
	}

	public double getSurplus() {
		return surplus;
	}

	public void setSurplus(double surplus) {
		this.surplus = surplus;
	}

}
