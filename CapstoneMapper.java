//***************************************************************
//
//  Developer:         Michael Franklin
//
//  Program #:         Capstone
//
//  File Name:         CapstoneMapper.java
//
//  Course:            COSC 3365 – Distributed Databases Using Hadoop 
//
//  Due Date:          05/13/22
//
//  Instructor:        Prof. Fred Kumi 
//
//  Description:
//					   Mapper class for Hadoop's Mapreduce
//
//***************************************************************

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CapstoneMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		public void map(LongWritable key, Text value, Context con)
		{
			try
			{
				String line = value.toString();
				String[] inputs = line.split(",");
				
				// create unique key using customer data
				Text outputKey = new Text(inputs[0] + "-" + inputs[1]);
				
				// check if returned
				int point = 0;
				boolean returned = inputs[6].equals("yes");
				
				// get return data if returned
				if (returned)
				{
					// parse received and returned dates
					SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
				    Date recvDate = dateFormat.parse(inputs[5]);
				    Date retnDate = dateFormat.parse(inputs[7]);

				    long differenceInMs = retnDate.getTime() - recvDate.getTime();
				    long differenceInDays = TimeUnit.DAYS.convert(differenceInMs, TimeUnit.MILLISECONDS);
				    
				    // check if day difference is greater than 10
				    if (differenceInDays > 10)
				    {
				    	// assign fraud point
				    	point = 1;
				    }
				}
				
				// package output to reducer
				Text outputValue = new Text((returned ? "1" : "0")
						+ "," + point);
				
				// send key pair to next step
				con.write(outputKey, outputValue);			
			}
			catch(Exception e)
			{
				// print out error and quick gracefully
				if (e instanceof IOException)
				{
					System.err.println("IOException encountered in Mapper class: CapstoneMapper:");
					e.printStackTrace();
					System.exit(0);
				}
				else if (e instanceof InterruptedException)
				{
					System.err.println("InterruptedException encountered in Mapper class: CapstoneMapper:");
					e.printStackTrace();
					System.exit(0);
				}
				else if (e instanceof NullPointerException)
				{
					System.err.println("NullPointerException encountered in Mapper class: CapstoneMapper:");
					e.printStackTrace();
					System.exit(0);
				}
				else if (e instanceof NumberFormatException)
				{
					System.err.println("NumberFormatException encountered in Mapper class: CapstoneMapper:");
					e.printStackTrace();
					System.exit(0);
				}
				else
				{
					System.err.println("Unhandled Exception encountered in Mapper class: CapstoneMapper:");
					e.printStackTrace();
					System.exit(0);
				}
			}
		}
	}
