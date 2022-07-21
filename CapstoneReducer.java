//***************************************************************
//
//  Developer:         Michael Franklin
//
//  Program #:         Capstone
//
//  File Name:         CapstoneReducer.java
//
//  Course:            COSC 3365 – Distributed Databases Using Hadoop 
//
//  Due Date:          05/13/22
//
//  Instructor:        Prof. Fred Kumi 
//
//  Description:
//					   Reducer class for Hadoop's Mapreduce
//
//***************************************************************

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CapstoneReducer extends Reducer<Text, Text, Text, Text>
	{
		private ArrayList<String> itemList = new ArrayList<>();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context con)
		{
			try
			{
				int fraudPoints = 0;
				double returnCount = 0;
				int orderCount = 0;
				// get the average of all success rates
				for (Text value : values)
				{
					// parse values
					String[] strs = value.toString().split(",");
					returnCount += Integer.parseInt(strs[0]);
					fraudPoints += Integer.parseInt(strs[1]);
					orderCount++;
				}
				
				// check for a high return rate
				double returnRate = (returnCount / orderCount) * 100;
				
				if (returnRate >= 50)
				{
					// assign 10 additional fraud points
					fraudPoints += 10;
				}
				
				// format label for output
				String[] labels = key.toString().split("-");
				String outputLabel = String.format(
						"%-20s%-15s",
						labels[0],
						labels[1]);
				 
				// send to output
				itemList.add(outputLabel + "," + fraudPoints);
				
			}
			catch(Exception e)
			{
				// print out error and quick gracefully
				if (e instanceof IOException)
				{
					System.err.println("IOException encountered in Reducer class: CapstoneReducer:");
					e.printStackTrace();
					System.exit(0);
				}
				else if (e instanceof InterruptedException)
				{
					System.err.println("InterruptedException encountered in Reducer class: CapstoneReducer:");
					e.printStackTrace();
					System.exit(0);
				}
				else
				{
					System.err.println("Unhandled Exception encountered in Reducer class: CapstoneReducer:");
					e.printStackTrace();
					System.exit(0);
				}
			}			
		}
		
		@Override
		public void cleanup(Context con)
		{
			// sort itemList
			itemList.sort(
					Collections.reverseOrder(Comparator.comparing(v1 ->
			Integer.parseInt(v1.split(",")[1]))));
			
			try 
			{
				// print headers
				con.write(new Text(String.format(
						"%-20s%-15s", "Customer #", "Customer Name")),
						new Text("Fraud Points"));
				
				for(int i = 0; i < itemList.size(); i++)
				{
					// extract label and value
					String[] strs = itemList.get(i).split(",");
					String outputLabel = strs[0];
					int fraudPoints = Integer.parseInt(strs[1]);
					
					// output label and value
					con.write(new Text(outputLabel),
						new Text(String.format(
								"%-5s%-15s","" + fraudPoints, "Pts")));
				}
			}
			catch(Exception e)
			{
				// print out error and quick gracefully
				if (e instanceof IOException)
				{
					System.err.println("IOException encountered during Cleanup: CapstoneReducer:");
					e.printStackTrace();
					System.exit(0);
				}
				else if (e instanceof InterruptedException)
				{
					System.err.println("InterruptedException encountered during Cleanup: CapstoneReducer:");
					e.printStackTrace();
					System.exit(0);
				}
				else
				{
					System.err.println("Unhandled Exception encountered during Cleanup: CapstoneReducer:");
					e.printStackTrace();
					System.exit(0);
				}
			}
		}
	}
