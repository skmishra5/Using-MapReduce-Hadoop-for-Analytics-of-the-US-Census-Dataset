package cs455.hadoop.answers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class QuestionEightExtraMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] token = value.toString().split("\t");
		
		context.write(new Text("SameKey"), new Text(token[0] + "\t" + token[9]));
		
	}
	
}
