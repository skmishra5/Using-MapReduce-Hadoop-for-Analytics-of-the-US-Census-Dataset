package cs455.hadoop.answers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class QuestionSegmentTwoReducer extends Reducer<Text, Text, Text, Text>{
	
	static ArrayList<String> ownerOccupiedHouseList = new ArrayList<String>(); // Question 5
	static ArrayList<String> rentHouseholdList = new ArrayList<String>(); // Question 6
	
	static{
		
		// Question 5
		
		ownerOccupiedHouseList.add("Less than $15,000");
		ownerOccupiedHouseList.add("$15,000 - $19,999");
		ownerOccupiedHouseList.add("$20,000 - $24,999");
		ownerOccupiedHouseList.add("$25,000 - $29,999");
		ownerOccupiedHouseList.add("$30,000 - $34,999");
		ownerOccupiedHouseList.add("$35,000 - $39,999");
		ownerOccupiedHouseList.add("$40,000 - $44,999");
		ownerOccupiedHouseList.add("$45,000 - $49,999");
		ownerOccupiedHouseList.add("$50,000 - $59,999");
		ownerOccupiedHouseList.add("$60,000 - $74,999");
		ownerOccupiedHouseList.add("$75,000 - $99,999");
		ownerOccupiedHouseList.add("$100,000 - $124,999");
		ownerOccupiedHouseList.add("$125,000 - $149,999");
		ownerOccupiedHouseList.add("$150,000 - $174,999");
		ownerOccupiedHouseList.add("$175,000 - $199,999");
		ownerOccupiedHouseList.add("$200,000 - $249,999");
		ownerOccupiedHouseList.add("$250,000 - $299,999");
		ownerOccupiedHouseList.add("$300,000 - $399,999");
		ownerOccupiedHouseList.add("$400,000 - $499,999");
		ownerOccupiedHouseList.add("$500,000 or more");
		
		// Question 6
		
		rentHouseholdList.add("Less than $100");
		rentHouseholdList.add("$100 to $149");
		rentHouseholdList.add("$150 to $199");
		rentHouseholdList.add("$200 to $249");
		rentHouseholdList.add("$250 to $299");
		rentHouseholdList.add("$300 to $349");
		rentHouseholdList.add("$350 to $399");
		rentHouseholdList.add("$400 to $449");
		rentHouseholdList.add("$450 to $499");
		rentHouseholdList.add("$500 to $549");
		rentHouseholdList.add("$550 to $ 599");
		rentHouseholdList.add("$600 to $649");
		rentHouseholdList.add("$650 to $699");
		rentHouseholdList.add("$700 to $749");
		rentHouseholdList.add("$750 to $999");
		rentHouseholdList.add("$1000 or more");
		
	}

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// Question 1
		long rentCount = 0;
        long ownedCount = 0;
        
        // Question 4
        long urban = 0;
        long rural = 0;
        long households = 0;
        
		ArrayList<Long> ownerOccupiedHouseNumber = new ArrayList<Long>(); // Question 5
		ArrayList<Long> rentalHouseholdNumber = new ArrayList<Long>(); // Question 6
		ArrayList<Long> roomList = new ArrayList<Long>(); // Question 7

        for(Text val : values){
        	
        	String[] mainToken = val.toString().trim().split("\t");
        	
        	// Question 1
        	long rentTemp = Long.parseLong(mainToken[0]);
        	long ownedTemp = Long.parseLong(mainToken[1]);
        	String[] token = mainToken[5].toString().trim().split("#"); // Question 5
        	String[] token1 = mainToken[6].toString().trim().split("#"); // Question 6
        	String[] token2 = mainToken[7].toString().trim().split("#"); // Question 7
        	
        	// Question 1
        	rentCount += rentTemp;
        	ownedCount += ownedTemp;
        	
        	// Question 4
        	households += Long.parseLong(mainToken[2]);
        	urban += Long.parseLong(mainToken[3]);
        	rural += Long.parseLong(mainToken[4]);
        	
        	// Question 5
        	if(ownerOccupiedHouseNumber.isEmpty()){
        		for(int i = 0; i < token.length; i++)
        		{
        			ownerOccupiedHouseNumber.add(Long.parseLong(token[i]));
        		}
        	}
        	else
        	{
        		for(int i = 0; i < token.length; i++)
        		{
        			long temp = ownerOccupiedHouseNumber.get(i);
        			temp += Long.parseLong(token[i]);
        			ownerOccupiedHouseNumber.set(i, temp);
        		}
        	}
        	
        	// Question 6
        	if(rentalHouseholdNumber.isEmpty()){
        		for(int i = 0; i < token1.length; i++)
        		{
        			rentalHouseholdNumber.add(Long.parseLong(token1[i]));
        		}
        	}
        	else
        	{
        		for(int i = 0; i < token1.length; i++)
        		{
        			long temp1 = rentalHouseholdNumber.get(i);
        			temp1 += Long.parseLong(token1[i]);
        			rentalHouseholdNumber.set(i, temp1);
        		}
        	}
        	
        	// Question 7
        	if(roomList.isEmpty()){
        		for(int i = 0; i < token2.length; i++)
        		{
        			roomList.add(Long.parseLong(token2[i]));
        		}
        	}
        	else
        	{
        		for(int i = 0; i < token2.length; i++)
        		{
        			long temp2 = roomList.get(i);
        			temp2 += Long.parseLong(token2[i]);
        			roomList.set(i, temp2);
        		}
        	}
        }
        
        // Question 1
        
        double percentageRented = rentCount * 100d / (rentCount + ownedCount);
        double percentageOwned = ownedCount * 100d / (rentCount + ownedCount);
        
        // Question 4
        
        double percentageRural = rural * 100d / households;
        double percentageUrban = urban * 100d/ households;
        
        // Question 5
        
        long medianSum = 0;
        for(Long num: ownerOccupiedHouseNumber)
        {
        	medianSum += num;
        }
        
        long medianHouse = calculateMedian(medianSum);
        int medianIndexInList = 0;
        
        for(long secondNum: ownerOccupiedHouseNumber)
        {
        	medianHouse -= secondNum;
        	if(medianHouse < 0)
        	{
        		medianIndexInList = ownerOccupiedHouseNumber.indexOf(secondNum);
        		break;
        	}
        }
        
        // Question 6
        
        long medianSum1 = 0;
        for(Long num1: rentalHouseholdNumber)
        {
        	medianSum1 += num1;
        }
        
        long medianHouse1 = calculateMedian(medianSum1);
        int medianIndexInList1 = 0;
        
        for(long secondNum1: rentalHouseholdNumber)
        {
        	medianHouse1 -= secondNum1;
        	if(medianHouse1 < 0)
        	{
        		medianIndexInList1 = rentalHouseholdNumber.indexOf(secondNum1);
        		break;
        	}
        }
        
        // Question 7
        int roomCount = 1;
        long totalNumberOfRooms = 0;
        long numberedRooms = 0;
        
        for(Long tempRoom: roomList)
        {
        	numberedRooms += tempRoom * roomCount;
        	totalNumberOfRooms += tempRoom;
        	++roomCount;
        }

        double averageNumRooms = (double) numberedRooms/totalNumberOfRooms;

        context.write(key, new Text("Q1(Rented): " + percentageRented + "\t" +"Q1(Owned): " + percentageOwned + "\t" + 
        		"Q4(Rural): " + percentageRural + "\t" + "Q4(Urban): " + percentageUrban + "\t" + "Q5: " + 
        		ownerOccupiedHouseList.get(medianIndexInList) + "\t" + "Q6: " + rentHouseholdList.get(medianIndexInList1) 
        		+ "\t" + averageNumRooms));
    }
	
	public long calculateMedian(long value)
	{
		if(value % 2 == 0)
		{
			return ((value/2) + 1);
		}
		else
		{
			return ((value + 1) / 2);
		}
	}
}
