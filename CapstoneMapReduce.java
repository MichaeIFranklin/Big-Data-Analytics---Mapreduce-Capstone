//***************************************************************
//
//  Developer:         Michael Franklin
//
//  Program #:         Capstone
//
//  File Name:         CapstoneMapReduce.java
//
//  Course:            COSC 3365 – Distributed Databases Using Hadoop 
//
//  Due Date:          05/13/22
//
//  Instructor:        Prof. Fred Kumi 
//
//  Description:
//					   Driver class for Hadoop's Mapreduce
//
//***************************************************************

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CapstoneMapReduce
{
	//***************************************************************
    //
    //  Method:       main
    // 
    //  Description:  The main method of the program
    //
    //  Parameters:   String array
    //
    //  Returns:      N/A 
    //
    //**************************************************************
	public static void main(String[] args)
	{
		try
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "CapstoneMapReduce");
			job.setJarByClass(CapstoneMapReduce.class);
			job.setMapperClass(CapstoneMapper.class);	
			job.setReducerClass(CapstoneReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		catch(Exception e)
		{
			// print out error and quick gracefully
			if (e instanceof IOException)
			{
				System.err.println("IOException encountered in Driver class: CapstoneMapReduce:");
				e.printStackTrace();
				System.exit(0);
			}
			else if (e instanceof ClassNotFoundException)
			{
				System.err.println("ClassNotFoundException encountered in Driver class: CapstoneMapReduce:");
				e.printStackTrace();
				System.exit(0);
			}
			else
			{
				System.err.println("Unhandled Exception encountered in Driver class: CapstoneMapReduce:");
				e.printStackTrace();
				System.exit(0);
			}
		}
	}
}
