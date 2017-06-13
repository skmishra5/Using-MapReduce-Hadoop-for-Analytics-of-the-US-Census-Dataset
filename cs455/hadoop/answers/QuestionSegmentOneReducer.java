package cs455.hadoop.answers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class QuestionSegmentOneReducer extends Reducer<Text, Text, Text, Text>{

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// Question 2
		long population = 0;
		long maleNeverMarried = 0;
		long femaleNeverMarried = 0;
		
		// Question 9
		long maleSeparated = 0;
		long femaleSeparated = 0;
		
		// Question 3
		long maleBelow18 = 0;
		long femaleBelow18 = 0;
		long maleBetween19To29 = 0;
		long femaleBetween19To29 = 0;
		long maleBetween30To39 = 0;
		long femaleBetween30To39 = 0;
		long malePopulation = 0l;
		long femalePopulation = 0l;
		
		// Question 8
		long totalAgedPopulation = 0; 
		long ageAbove85Number = 0;
		//double agePercentage = 0.0d;
		
		for(Text val : values){
    		
			String[] mainToken = val.toString().trim().split("\t");
			
			// Question 2
			population += Long.parseLong(mainToken[0]);
        	maleNeverMarried += Long.parseLong(mainToken[1]);
        	femaleNeverMarried += Long.parseLong(mainToken[2]);
        	
        	// Question 9
        	maleSeparated += Long.parseLong(mainToken[13]);
        	femaleSeparated += Long.parseLong(mainToken[14]);
        	
        	// Question 3
        	malePopulation += Long.parseLong(mainToken[3]);
        	maleBelow18 += Long.parseLong(mainToken[4]);
        	maleBetween19To29 += Long.parseLong(mainToken[5]);
        	maleBetween30To39 += Long.parseLong(mainToken[6]);
        	femalePopulation += Long.parseLong(mainToken[7]);
        	femaleBelow18 += Long.parseLong(mainToken[8]);
        	femaleBetween19To29 += Long.parseLong(mainToken[9]);
        	femaleBetween30To39 += Long.parseLong(mainToken[10]);
        	
			// Question 8
    		totalAgedPopulation += Long.parseLong(mainToken[11]);
    		ageAbove85Number += Long.parseLong(mainToken[12]);
		}
		
		
		
		// Question 2
		double malePercentage = maleNeverMarried * 100d / population;
		double femalePercentage = femaleNeverMarried * 100d / population;
		
		// Question 9
		double maleSepPercent = maleSeparated * 100d / population;
		double femaleSepPercent = femaleSeparated * 100d / population;
		
		// Question 3
		double maleBelow18Percentage = maleBelow18 * 100d / malePopulation;
		double male19To29Percentage = maleBetween19To29 * 100d / malePopulation;
		double male30to39Percentage = maleBetween30To39 * 100d / malePopulation;
		double femaleBelow18Percentage = femaleBelow18 * 100d / femalePopulation;
		double female19To29Percentage = femaleBetween19To29 * 100d / femalePopulation;
		double female30to39Percentage = femaleBetween30To39 * 100d / femalePopulation;
		
		
		// Question 8
        double agePercentage = ageAbove85Number * 100d / totalAgedPopulation;
        
        context.write(key, new Text("Q2M: " + malePercentage + "\t" + "Q2F: " + femalePercentage + "\t" + "Q3(a)M: " + 
        		maleBelow18Percentage + "\t" + "Q3(b)M: " + male19To29Percentage + "\t" + "Q3(c)M: " + male30to39Percentage 
        		+ "\t" + "Q3(a)F: " + femaleBelow18Percentage + "\t" + "Q3(b)F: " + female19To29Percentage + "\t" + "Q3(c)F: " +
        		female30to39Percentage + "\t" + String.valueOf(agePercentage) + "\t" + "Q9M: " + maleSepPercent 
        		+ "\t" + "Q9F: " + femaleSepPercent));
	}
	
}
