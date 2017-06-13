package cs455.hadoop.answers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class QuestionEightExtraReducer extends Reducer<Text, Text, Text, Text>{
	
	double highestAgedPercentage = 0.0d;
	String highestAgedState = "";
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// Question 8
		double tempVal = 0.0d;
		
		for(Text val : values){
			
			String[] token = val.toString().split("\t");
			
			// Question 8
			tempVal = Double.parseDouble(token[1]);
			if(tempVal > highestAgedPercentage)
			{
				highestAgedPercentage = tempVal;
				highestAgedState = token[0];
			}
		}
		
		context.write(new Text("Highest Percentage State Of Elderly People (age > 85) in their Population "), 
				new Text(highestAgedState + ""));
	}

}
