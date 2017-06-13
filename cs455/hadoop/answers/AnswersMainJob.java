package cs455.hadoop.answers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AnswersMainJob {

	public static void main(String[] args) {
		
		// Answer to Questions with segment number as 1
		try {
		     Configuration conf = new Configuration();
		     // Give the MapRed job a name. You'll see this name in the Yarn webapp.
		     Job job = Job.getInstance(conf, "Q2/3/8/9 Answers");	
		     // Current class.
		     job.setJarByClass(AnswersMainJob.class);
		     // Mapper
		     job.setMapperClass(QuestionSegmentOneMapper.class);
		     // Combiner. We use the reducer as the combiner in this case.
		     //job.setCombinerClass(QuestionOneReducer.class);
		     // Reducer
		     job.setReducerClass(QuestionSegmentOneReducer.class);
		     // Outputs from the Mapper.
		     job.setMapOutputKeyClass(Text.class);
		     job.setMapOutputValueClass(Text.class);
		     // Outputs from Reducer. It is sufficient to set only the following two properties
		     // if the Mapper and Reducer has same key and value types. It is set separately for
		     // elaboration.
		     job.setOutputKeyClass(Text.class);
		     job.setOutputValueClass(Text.class);
		     // path to input in HDFS
		     FileInputFormat.addInputPath(job, new Path(args[0]));
		     // path to output in HDFS
		     FileOutputFormat.setOutputPath(job, new Path(args[1]));
		     // Block until the job is completed.
		     if(job.waitForCompletion(true))
             {
                 System.out.println("First Job completed");
             }
		} catch (IOException e) {
		     System.err.println(e.getMessage());
		} catch (InterruptedException e) {
		     System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
		     System.err.println(e.getMessage());
		}
	
		
		//Answer to Questions with segment number 2
		try {
		     Configuration conf1 = new Configuration();
		     // Give the MapRed job a name. You'll see this name in the Yarn webapp.
		     Job job1 = Job.getInstance(conf1, "Q4/5/6/7 Answers");
		     // Current class.
		     job1.setJarByClass(AnswersMainJob.class);
		     // Mapper
		     job1.setMapperClass(QuestionSegmentTwoMapper.class);
		     // Combiner. We use the reducer as the combiner in this case.
		     //job4.setCombinerClass(QuestionFiveCombiner.class);
		     // Reducer
		     job1.setReducerClass(QuestionSegmentTwoReducer.class);
		     // Outputs from the Mapper.
		     job1.setMapOutputKeyClass(Text.class);
		     job1.setMapOutputValueClass(Text.class);
		     // Outputs from Reducer. It is sufficient to set only the following two properties
		     // if the Mapper and Reducer has same key and value types. It is set separately for
		     // elaboration.
		     job1.setOutputKeyClass(Text.class);
		     job1.setOutputValueClass(Text.class);
		     // path to input in HDFS
		     FileInputFormat.addInputPath(job1, new Path(args[0]));
		     // path to output in HDFS
		     FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		     // Block until the job is completed.
		     if(job1.waitForCompletion(true))
             {
                 System.out.println("Second Job completed");
             }
		     
		} catch (IOException e) {
		     System.err.println(e.getMessage());
		} catch (InterruptedException e) {
		     System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
		     System.err.println(e.getMessage());
		}
		
		// Question 7 extra Job
		try {
		     Configuration conf2 = new Configuration();
		     // Give the MapRed job a name. You'll see this name in the Yarn webapp.
		     Job job2 = Job.getInstance(conf2, "Q7 Answers");
		     // Current class.
		     job2.setJarByClass(AnswersMainJob.class);
		     // Mapper
		     job2.setMapperClass(QuestionSevenExtraMapper.class);
		     // Combiner. We use the reducer as the combiner in this case.
		     //job4.setCombinerClass(QuestionFiveCombiner.class);
		     // Reducer
		     job2.setReducerClass(QuestionSevenExtraReducer.class);
		     // Outputs from the Mapper.
		     job2.setMapOutputKeyClass(Text.class);
		     job2.setMapOutputValueClass(Text.class);
		     // Outputs from Reducer. It is sufficient to set only the following two properties
		     // if the Mapper and Reducer has same key and value types. It is set separately for
		     // elaboration.
		     job2.setOutputKeyClass(Text.class);
		     job2.setOutputValueClass(Text.class);
		     // path to input in HDFS
		     FileInputFormat.addInputPath(job2, new Path(args[2]));
		     // path to output in HDFS
		     FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		     // Block until the job is completed.
		     if(job2.waitForCompletion(true))
             {
                 System.out.println("Third Job completed");
             }
		} catch (IOException e) {
		     System.err.println(e.getMessage());
		} catch (InterruptedException e) {
		     System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
		     System.err.println(e.getMessage());
		}
		
		// Question 8 extra Job
		try {
		     Configuration conf3 = new Configuration();
		     // Give the MapRed job a name. You'll see this name in the Yarn webapp.
		     Job job3 = Job.getInstance(conf3, "Q8 Answers");
		     // Current class.
		     job3.setJarByClass(AnswersMainJob.class);
		     // Mapper
		     job3.setMapperClass(QuestionEightExtraMapper.class);
		     // Combiner. We use the reducer as the combiner in this case.
		     //job4.setCombinerClass(QuestionFiveCombiner.class);
		     // Reducer
		     job3.setReducerClass(QuestionEightExtraReducer.class);
		     // Outputs from the Mapper.
		     job3.setMapOutputKeyClass(Text.class);
		     job3.setMapOutputValueClass(Text.class);
		     // Outputs from Reducer. It is sufficient to set only the following two properties
		     // if the Mapper and Reducer has same key and value types. It is set separately for
		     // elaboration.
		     job3.setOutputKeyClass(Text.class);
		     job3.setOutputValueClass(Text.class);
		     // path to input in HDFS
		     FileInputFormat.addInputPath(job3, new Path(args[1]));
		     // path to output in HDFS
		     FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		     // Block until the job is completed.
		     if(job3.waitForCompletion(true))
             {
                 System.out.println("Fourth Job completed");
             }
		} catch (IOException e) {
		     System.err.println(e.getMessage());
		} catch (InterruptedException e) {
		     System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
		     System.err.println(e.getMessage());
		}
		
		try {
		     Configuration conf4 = new Configuration();
		     // Give the MapRed job a name. You'll see this name in the Yarn webapp.
		     Job job4 = Job.getInstance(conf4, "Q9 Answers");
		     // Current class.
		     job4.setJarByClass(AnswersMainJob.class);
		     // Mapper
		     job4.setMapperClass(QustionNineSegTwoMapper.class);
		     // Combiner. We use the reducer as the combiner in this case.
		     //job4.setCombinerClass(QuestionFiveCombiner.class);
		     // Reducer
		     job4.setReducerClass(QuestionNineSegTwoReducer.class);
		     // Outputs from the Mapper.
		     job4.setMapOutputKeyClass(Text.class);
		     job4.setMapOutputValueClass(Text.class);
		     // Outputs from Reducer. It is sufficient to set only the following two properties
		     // if the Mapper and Reducer has same key and value types. It is set separately for
		     // elaboration.
		     job4.setOutputKeyClass(Text.class);
		     job4.setOutputValueClass(Text.class);
		     // path to input in HDFS
		     FileInputFormat.addInputPath(job4, new Path(args[0]));
		     // path to output in HDFS
		     FileOutputFormat.setOutputPath(job4, new Path(args[5]));
		     // Block until the job is completed.
		     System.exit(job4.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
		     System.err.println(e.getMessage());
		} catch (InterruptedException e) {
		     System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
		     System.err.println(e.getMessage());
		}
	}

}
