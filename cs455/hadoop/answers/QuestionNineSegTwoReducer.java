package cs455.hadoop.answers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class QuestionNineSegTwoReducer extends Reducer<Text, Text, Text, Text>{
	
	static ArrayList<String> ownerRenterOccupiedRaceList = new ArrayList<String>();
	static ArrayList<String> ownerRenterAgeGroupList = new ArrayList<String>();
	int ownerPosition = 0;
	int renterPosition = 0;
	int ownerAgePosition = 0;
	int renterAgePosition = 0;
	
	static{
		ownerRenterOccupiedRaceList.add("White");
		ownerRenterOccupiedRaceList.add("Black");
		ownerRenterOccupiedRaceList.add("American Indian, Eskimo, or Aleut");
		ownerRenterOccupiedRaceList.add("Asian or Pacific Islander");
		ownerRenterOccupiedRaceList.add("Other Race");
		ownerRenterAgeGroupList.add("15 to 24 years");
		ownerRenterAgeGroupList.add("25 to 34 years");
		ownerRenterAgeGroupList.add("35 to 44 years");
		ownerRenterAgeGroupList.add("45 to 54 years");
		ownerRenterAgeGroupList.add("55 to 64 years");
		ownerRenterAgeGroupList.add("65 to 74 years");
		ownerRenterAgeGroupList.add("75 years and over");
	}
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		ArrayList<Long> ownerOccupiedRaceList = new ArrayList<Long>();
		ArrayList<Long> renterOccupiedRaceList = new ArrayList<Long>();
		ArrayList<Long> ownerAgeList = new ArrayList<Long>();
		ArrayList<Long> renterAgeList = new ArrayList<Long>();
		
		for(Text val : values){
        	
        	String[] mainToken = val.toString().trim().split("\t");
        	
        	String[] token = mainToken[0].toString().trim().split("#");
        	String[] token1 = mainToken[1].toString().trim().split("#");
        	String[] token2 = mainToken[2].toString().trim().split("#");
        	String[] token3 = mainToken[3].toString().trim().split("#");
        	
        	// For Owner's Race
        	if(ownerOccupiedRaceList.isEmpty()){
        		for(int i = 0; i < token.length; i++)
        		{
        			ownerOccupiedRaceList.add(Long.parseLong(token[i]));
        		}
        	}
        	else
        	{
        		for(int i = 0; i < token.length; i++)
        		{
        			long temp = ownerOccupiedRaceList.get(i);
        			temp += Long.parseLong(token[i]);
        			ownerOccupiedRaceList.set(i, temp);
        		}
        	}
        	
        	// For Renter's Race
        	if(renterOccupiedRaceList.isEmpty()){
        		for(int i = 0; i < token1.length; i++)
        		{
        			renterOccupiedRaceList.add(Long.parseLong(token1[i]));
        		}
        	}
        	else
        	{
        		for(int i = 0; i < token1.length; i++)
        		{
        			long temp1 = renterOccupiedRaceList.get(i);
        			temp1 += Long.parseLong(token1[i]);
        			renterOccupiedRaceList.set(i, temp1);
        		}
        	}
        	
        	// For Owner's Age group
        	if(ownerAgeList.isEmpty()){
        		for(int i = 0; i < token2.length; i++)
        		{
        			ownerAgeList.add(Long.parseLong(token2[i]));
        		}
        	}
        	else
        	{
        		for(int i = 0; i < token2.length; i++)
        		{
        			long temp2 = ownerAgeList.get(i);
        			temp2 += Long.parseLong(token2[i]);
        			ownerAgeList.set(i, temp2);
        		}
        	}
        	
        	// For Renter's Age group
        	if(renterAgeList.isEmpty()){
        		for(int i = 0; i < token3.length; i++)
        		{
        			renterAgeList.add(Long.parseLong(token3[i]));
        		}
        	}
        	else
        	{
        		for(int i = 0; i < token3.length; i++)
        		{
        			long temp3 = renterAgeList.get(i);
        			temp3 += Long.parseLong(token3[i]);
        			renterAgeList.set(i, temp3);
        		}
        	}
		}
		
		// Owner Max
		long ownerValue = 0;
		long totalOwnerCount = 0;
		for(int i = 0; i < ownerOccupiedRaceList.size(); i++)
		{
			totalOwnerCount += ownerOccupiedRaceList.get(i);
			if(ownerOccupiedRaceList.get(i) > ownerValue)
			{
				ownerValue = ownerOccupiedRaceList.get(i);
				ownerPosition = i;
			}
		}
		
		// Renter Max
		long renterValue = 0;
		long totalRenterCount = 0;
		for(int i = 0; i < renterOccupiedRaceList.size(); i++)
		{
			totalRenterCount += renterOccupiedRaceList.get(i);
			if(renterOccupiedRaceList.get(i) > renterValue)
			{
				renterValue = renterOccupiedRaceList.get(i);
				renterPosition = i;
			}
		}
		
		// Owner Age Group Max
		long ownerAgeValue = 0;
		long totalOwnerAgeCount = 0;
		for(int i = 0; i < ownerAgeList.size(); i++)
		{
			totalOwnerAgeCount += ownerAgeList.get(i);
			if(ownerAgeList.get(i) > ownerAgeValue)
			{
				ownerAgeValue = ownerAgeList.get(i);
				ownerAgePosition = i;
			}
		}
		
		// Renter Age Group Max
		long renterAgeValue = 0;
		long totalRenterAgeCount = 0;
		for(int i = 0; i < renterAgeList.size(); i++)
		{
			totalRenterAgeCount += renterAgeList.get(i);
			if(renterAgeList.get(i) > renterAgeValue)
			{
				renterAgeValue = renterAgeList.get(i);
				renterAgePosition = i;
			}
		}
		
		double ownerMajorityPercentage = ownerValue * 100d / totalOwnerCount;
		double renterMajorityPercentage = renterValue * 100d / totalRenterCount;
		double ownerRenterAgeGroupPercentage = ownerAgeValue * 100d / totalOwnerAgeCount;
		double renterAgeMajorityPercentage = renterAgeValue * 100d / totalRenterAgeCount;
		
		context.write(key, new Text("Owner Race Majority: " + ownerRenterOccupiedRaceList.get(ownerPosition) + "\t\t" 
				+ "Owner Race Majority Percentage: " + ownerMajorityPercentage + "\t" + "Renter Race Majority: " + 
				ownerRenterOccupiedRaceList.get(renterPosition) + "\t\t" + "Renter Race Majority Percentage: " + 
				renterMajorityPercentage + "\t" + "Owner Age group Majority: " + ownerRenterAgeGroupList.get(ownerAgePosition)
				+ "\t\t" + "Owner Age Majority Percentage: " + ownerRenterAgeGroupPercentage + "\t" + "Renter Age Group Majority: "
				+ ownerRenterAgeGroupList.get(renterAgePosition) + "\t\t" + "Renter Age Majority Percentage: " + 
				renterAgeMajorityPercentage));
		
	}

}
