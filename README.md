# Using-MapReduce-Hadoop-for-Analytics-of-the-US-Census-Dataset

Instructions to execute the Project
-----------------------------------
-----------------------------------

1. The build.xml is kept at the "cs455_HW3/" directory.

2. Please run "ant" in order to build the project.

3. The hadoop cluster should be up and running.

4. In order to access the shared data, it is required to set the path "export HADOOP_CONF_DIR=<path of client config>"

5. Then run the following command to run the jar which takes 6 arguments.

"$HADOOP_HOME/bin/hadoop jar dist/hw3Answers.jar cs455.hadoop.answers.AnswersMainJob /data/census /home/cs455/name1 /home/cs455/name2 /home/cs455/name3 /home/cs455/name4 /home/cs455/name5"




File Description
----------------
----------------

Package "cs455.hadoop.answers"
------------------------------

1. AnswersMainJob.java: This class contains the main function which contains 5 jobs. The third job answers question 7 and fourth job answers question 8. The fifth job answers question 9.

2. QuestionSegmentOneMapper.java: This mapper class answers question 2,3,8 and some part of question9. It takes state as key and others as values.

3. QuestionSegmentOneReducer.java: This reducer class answers question 2,3,8 and part of question9. Question 8's intermediate results are printed here.

4. QuestionSegmentTwoMapper.java: This mapper class answers question 1,4,5,6,7.

5. QuestionSegmentTwoReducer.java: This reducer class answers question 1,4,5,6,7. Question 7's intermediate results are printed here.

6. QuestionSevenExtraMapper.java: This mapper class reads the third command line argument as input.

7. QuestionSevenExtraReducer.java: This reducer class computes the 95 percetile of average number of houses accross all the states.

8. QuestionEightExtraMapper.java: This mapper class reads the second command line argument as input.

9. QuestionEightExtraReducer.java: This reducer class computes the highest percentage of elderly people (age > 85) in their population.

10. QustionNineSegTwoMapper.java: This mapper class answers question 9.

11. QuestionNineSegTwoReducer.java: This reducer class answers question 9.  



