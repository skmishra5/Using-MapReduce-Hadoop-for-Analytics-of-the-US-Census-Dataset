package cs455.hadoop.answers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class QuestionSegmentOneMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	int startIndexQuestionEight = 795;
	int startIndexQuestionThreeMale = 3864;
	int startIndexQuestionThreeFemale = 4143;
	
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String segment = value.toString().substring(24, 28);

		if(Integer.parseInt(segment) == 1)
		{
			String summeryLevel = value.toString().substring(10, 13);
			String state = "";
			// Question 8
			long agePopulation = 0;
			long ageAbove85 = 0; 
			
			if(Integer.parseInt(summeryLevel) == 100)
			{
				state = value.toString().substring(8, 10);
				
				// Question 2
				
				String population = value.toString().substring(300, 309);
				String maleNeverMarried = value.toString().substring(4422, 4431);
				String femaleNeverMarried = value.toString().substring(4467, 4476);
				
				// Question 9
				String maleSeparated = value.toString().substring(4440, 4449);
				String femaleSeparated = value.toString().substring(4485, 4494);
				
				// Question 3
				
				long maleBelow18 = 0;
				long femaleBelow18 = 0;
				long maleBetween19To29 = 0;
				long femaleBetween19To29 = 0;
				long maleBetween30To39 = 0;
				long femaleBetween30To39 = 0;
				long malePopulation = 0l;
				long femalePopulation = 0l;
				
				// Male population
				for(int i = 1; i <= 31; i++)
				{
					int start = startIndexQuestionThreeMale + ((i - 1) * 9);
					int end = start + 9;
					long tempMaleAge = Long.parseLong(value.toString().substring(start, end));
					malePopulation += tempMaleAge;
					
					if(i <= 13)
					{
						maleBelow18 += tempMaleAge;
					}
					else if(i <= 18)
					{
						maleBetween19To29 += tempMaleAge;
					}
					else if(i <= 20)
					{
						maleBetween30To39 += tempMaleAge;
					}					
				}
				
				// Female population
				for(int i = 1; i <= 31; i++)
				{
					int start = startIndexQuestionThreeFemale + ((i - 1) * 9);
					int end = start + 9;
					long tempFemaleAge = Long.parseLong(value.toString().substring(start, end));
					femalePopulation += tempFemaleAge;
					
					if(i <= 13)
					{
						femaleBelow18 += tempFemaleAge;
					}
					else if(i <= 18)
					{
						femaleBetween19To29 += tempFemaleAge;
					}
					else if(i <= 20)
					{
						femaleBetween30To39 += tempFemaleAge;
					}					
				}
				
				
				// Question 8
				for(int i = 1; i <= 31; i++)
				{
					int start = startIndexQuestionEight + ((i - 1) * 9);
					int end = start + 9;
					agePopulation += Long.parseLong(value.toString().substring(start, end));
					
					if(i == 31)
					{
						ageAbove85 = Long.parseLong(value.toString().substring(start, end));
					}
					
				}
				context.write(new Text(state), new Text(population + "\t" + maleNeverMarried + "\t" + femaleNeverMarried + "\t" + 
						malePopulation + "\t" + maleBelow18 + "\t" + maleBetween19To29 + "\t" + maleBetween30To39 + "\t" + 
						femalePopulation + "\t" + femaleBelow18 + "\t" + femaleBetween19To29 + "\t" + femaleBetween30To39 +
						"\t" + agePopulation + "\t" + ageAbove85 + "\t" + maleSeparated + "\t" + femaleSeparated));
			}
			
			
			
		}
		
		
	}
}
