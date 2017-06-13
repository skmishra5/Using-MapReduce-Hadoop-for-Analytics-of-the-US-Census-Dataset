package cs455.hadoop.answers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class QustionNineSegTwoMapper extends Mapper<LongWritable, Text, Text, Text>{

	int startIndexOwnerRace = 2172;
	int startIndexRenterRace = 2217;
	int startIndexOwnerAgeGroup = 2262;
	int startIndexRenterAgeGroup = 2325;
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String segment = value.toString().substring(24, 28);
		
		if(Integer.parseInt(segment) == 2)
		{
			String summeryLevel = value.toString().substring(10, 13);
			String state = "";
			String ownerRace = "";
			String renterRace = "";
			String ownerAge = "";
			String renterAge = "";
			
			if(Integer.parseInt(summeryLevel) == 100)
			{
				state = value.toString().substring(8, 10);
				
				// Getting Owner Occupied Race
				for(int i = 1; i <= 5; i++)
				{
					int start = startIndexOwnerRace + ((i - 1) * 9);
					int end = start + 9;
					if(i == 5)
					{
						ownerRace += Long.parseLong(value.toString().substring(start, end));
					}
					else{
						ownerRace += Long.parseLong(value.toString().substring(start, end))+ "#";
					}
				}
				
				// Getting Renter Occupied Race
				for(int i = 1; i <= 5; i++)
				{
					int start = startIndexRenterRace + ((i - 1) * 9);
					int end = start + 9;
					if(i == 5)
					{
						renterRace += Long.parseLong(value.toString().substring(start, end));
					}
					else{
						renterRace += Long.parseLong(value.toString().substring(start, end))+ "#";
					}
				}
				
				// Getting Owner Age Group
				for(int i = 1; i <= 7; i++)
				{
					int start = startIndexOwnerAgeGroup + ((i - 1) * 9);
					int end = start + 9;
					if(i == 7)
					{
						ownerAge += Long.parseLong(value.toString().substring(start, end));
					}
					else{
						ownerAge += Long.parseLong(value.toString().substring(start, end))+ "#";
					}
				}
				
				// Getting Renter Age Group
				for(int i = 1; i <= 7; i++)
				{
					int start = startIndexRenterAgeGroup + ((i - 1) * 9);
					int end = start + 9;
					if(i == 7)
					{
						renterAge += Long.parseLong(value.toString().substring(start, end));
					}
					else{
						renterAge += Long.parseLong(value.toString().substring(start, end))+ "#";
					}
				}
				
				context.write(new Text(state), new Text(ownerRace + "\t" + renterRace + "\t" + ownerAge + "\t" + renterAge));
			}
		}
	}
	
}
