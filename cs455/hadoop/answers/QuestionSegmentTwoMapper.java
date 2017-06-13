package cs455.hadoop.answers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class QuestionSegmentTwoMapper extends Mapper<LongWritable, Text, Text, Text>{

	int startIndexQuestionFive = 2928;
	int startIndexQuestionSix = 3450;
	int startIndexQuestionSeven = 2388;

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String segment = value.toString().substring(24, 28);

		if(Integer.parseInt(segment) == 2)
		{
			String summeryLevel = value.toString().substring(10, 13);
			String state = "";
			String ownerOccupiedHouses = ""; // Question 5
			String rentPaidByHouseholds = ""; // Question 6
			String avgNumRooms = ""; // Question 7
			
			if(Integer.parseInt(summeryLevel) == 100)
			{
				state = value.toString().substring(8, 10);
				
				// Question 1
				String owned = value.toString().substring(1803, 1812);
				String rented = value.toString().substring(1812, 1821);
				
				// Question 4
				long urban = 0;
				urban += Long.parseLong(value.toString().substring(1821, 1830));
				urban += Long.parseLong(value.toString().substring(1830, 1839));
				long rural = Long.parseLong(value.toString().substring(1839, 1848));
				long notDefined = Long.parseLong(value.toString().substring(1848, 1857));

				long households = urban + rural + notDefined;
				//housholds += Long.parseLong(value.toString().substring(1848, 1857));
				
				// Question 5
				for(int i = 1; i <= 20; i++)
				{
					int start = startIndexQuestionFive + ((i - 1) * 9);
					int end = start + 9;
					if(i == 20)
					{
						ownerOccupiedHouses += Long.parseLong(value.toString().substring(start, end));
					}
					else{
						ownerOccupiedHouses += Long.parseLong(value.toString().substring(start, end))+ "#";
					}
				}
				
				// Question 6
				for(int i = 1; i <= 16; i++)
				{
					int start = startIndexQuestionSix + ((i - 1) * 9);
					int end = start + 9;
					if(i == 16)
					{
						rentPaidByHouseholds += Long.parseLong(value.toString().substring(start, end));
					}
					else{
						rentPaidByHouseholds += Long.parseLong(value.toString().substring(start, end))+ "#";
					}
				}
				
				// Question 7
				for(int i = 1; i <= 9; i++)
				{
					int start = startIndexQuestionSeven + ((i - 1) * 9);
					int end = start + 9;
					if(i == 9)
					{
						avgNumRooms += Long.parseLong(value.toString().substring(start, end));
					}
					else{
						avgNumRooms += Long.parseLong(value.toString().substring(start, end))+ "#";
					}
				}
				
				context.write(new Text(state), new Text(rented + "\t" + owned + "\t" + households + "\t" + urban + "\t" + 
						rural + "\t" + ownerOccupiedHouses + "\t" + rentPaidByHouseholds + "\t" + avgNumRooms));
			}
			
		}

    }
}
