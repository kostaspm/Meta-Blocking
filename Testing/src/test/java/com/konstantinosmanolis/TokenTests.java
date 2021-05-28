package com.konstantinosmanolis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;

public class TokenTests {

	public static void main(String[] args) {
		
		try
		 {
		 
		 //csv file containing data
		 String strFile = "data/AmazonGoogle/Amazon.csv";
		 
		 //create BufferedReader to read csv file
		 BufferedReader br = new BufferedReader( new FileReader(strFile));
		 String strLine = "";
		 StringTokenizer st = null;
		 int lineNumber = 0, tokenNumber = 0;
		 
		 //read comma separated file line by line
		 while( (strLine = br.readLine()) != null)
		 {
		 lineNumber++;
		 
		 //break comma separated line using ","
		 st = new StringTokenizer(strLine, ",");
		 
		 while(st.hasMoreTokens())
		 {
		 //display csv values
		 tokenNumber++;
		 System.out.println("Line # " + lineNumber + 
		 ", Token # " + tokenNumber 
		 + ", Token : "+ st.nextToken());
		 }
		 
		 //reset token number
		 tokenNumber = 0;
		 
		 }
		 
		 
		 }
		 catch(Exception e)
		 {
		 System.out.println("Exception while reading csv file: " + e); 
		 }
	}

}
