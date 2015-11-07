package com.lee.hadoop.mr.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class InfoBean implements WritableComparable<InfoBean> {

	private String account;				// �˺�
	private double income;			// ����
	private double expenses;		// ֧��
	private double surplus;			// ����
	
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
		// ��spring�п���Ϊ����
//		NumberFormat currency = NumberFormat.getCurrencyInstance(); //�������Ҹ�ʽ������ 
//	    NumberFormat percent = NumberFormat.getPercentInstance();  //�����ٷֱȸ�ʽ������ 
//	    percent.setMaximumFractionDigits(3); //�ٷֱ�С�������3λ 
	    
//		return currency.format(income) + "\t" + currency.format(expenses) + "\t" + currency.format(surplus);
		return this.income + "\t" + this.expenses + "\t" + this.surplus;
	}

	// ����1���Ǵ�С��������
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
