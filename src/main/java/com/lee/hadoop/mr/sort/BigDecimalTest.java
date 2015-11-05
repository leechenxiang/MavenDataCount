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
		
		
		NumberFormat currency = NumberFormat.getCurrencyInstance(); //建立货币格式化引用 
	    NumberFormat percent = NumberFormat.getPercentInstance();  //建立百分比格式化引用 
	    percent.setMaximumFractionDigits(3); //百分比小数点最多3位 	控制小数点位数
	    
	    BigDecimal loanAmount = new BigDecimal("15000.48"); //贷款金额
	    BigDecimal interestRate = new BigDecimal("0.008"); //利率   
	    BigDecimal interest = loanAmount.multiply(interestRate); //相乘

	    System.out.println("贷款金额:\t" + currency.format(loanAmount)); 
	    System.out.println("利率:\t" + percent.format(interestRate)); 
	    System.out.println("利息:\t" + currency.format(interest)); 
	}

}
