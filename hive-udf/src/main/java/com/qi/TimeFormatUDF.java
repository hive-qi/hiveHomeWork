package com.qi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.exec.UDF;


public class TimeFormatUDF extends UDF{
	
	public static final SimpleDateFormat DATE_FORMAT=new SimpleDateFormat("[dd/MMMMM/yyyy:HH:mm:ss Z]",Locale.ENGLISH);
	public static final SimpleDateFormat STAND_DATE_FORMAT=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public String evaluate(String s) {
		
		try {
			Date oDate = DATE_FORMAT.parse(s);
			return STAND_DATE_FORMAT.format(oDate);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String evaluate(String s,String type) {
		
		try {
			Date odate=DATE_FORMAT.parse(s);
			if (type!=null && type.equals("hour")) {
				SimpleDateFormat sdf=new SimpleDateFormat("HH");
				return sdf.format(odate);
			}else if (type.equals("year")) {
				SimpleDateFormat sdf=new SimpleDateFormat("yyyy");
				return sdf.format(odate);
			}else if (type.equals("month")) {
				SimpleDateFormat sdf=new SimpleDateFormat("MM");
				return sdf.format(odate);
			}else if (type.equals("day")) {
				SimpleDateFormat sdf=new SimpleDateFormat("dd");
				return sdf.format(odate);
			}else {
				return null;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	
	}
	
	public static void main(String[] args) {
		TimeFormatUDF test=new TimeFormatUDF();
		String result=test.evaluate("[29/April/2016:17:38:20 +0800]");
		System.out.println(result);
	}

}
