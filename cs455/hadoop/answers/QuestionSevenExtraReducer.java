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

public class QuestionSevenExtraReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override	
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		ArrayList<Double> avgRoomList = new ArrayList<Double>(); // Question 7
		
		for(Text val : values){
			
			String[] token = val.toString().split("\t");
			avgRoomList.add(Double.parseDouble(token[0])); // Question 7
			
		}
		
		Collections.sort(avgRoomList);
		long ninetyFivePercent = Math.round(0.95 * avgRoomList.size());
		double percentile = avgRoomList.get((int) (ninetyFivePercent + 1));
		
		context.write(new Text("95 Percentile: "), new Text(percentile + ""));
		
	}

}
