package com.lee.hadoop.mr.sort;

import java.math.BigDecimal;
import java.text.NumberFormat;

public class BigDecimalTest {

	public static void main(String[] args) {
//		BigDecimal a = new BigDecimal(2.22);
//		BigDecimal b = new BigDecimal(1.01);
//		
//		double c = a.subtract(b).doubleValue();
//		System.out.println(c);
		
		
		NumberFormat currency = NumberFormat.getCurrencyInstance(); //�������Ҹ�ʽ������ 
	    NumberFormat percent = NumberFormat.getPercentInstance();  //�����ٷֱȸ�ʽ������ 
	    percent.setMaximumFractionDigits(3); //�ٷֱ�С�������3λ 	����С����λ��
	    
	    BigDecimal loanAmount = new BigDecimal("15000.48"); //������
	    BigDecimal interestRate = new BigDecimal("0.008"); //����   
	    BigDecimal interest = loanAmount.multiply(interestRate); //���

	    System.out.println("������:\t" + currency.format(loanAmount)); 
	    System.out.println("����:\t" + percent.format(interestRate)); 
	    System.out.println("��Ϣ:\t" + currency.format(interest)); 
	}

}
