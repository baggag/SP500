# Solution for SP500

# Assumptions
Input file is in CSV format

SBT build tool is used.

cd to Project Home Directory
## To run test cases:
sbt test

## To package the jar file built to path target/scala-2.11/sp500_2.11-1.0.jar
sbt package

## Usage:
spark-submit  --class "com.scotiabank.sp.SP500Analysis" --master local[*] <path_to_jar_file>  <path_to_SP500_csv> <probability_in_double>

## To run
spark-submit --class "com.scotiabank.sp.SP500Analysis" --master local[*] ~/SP500/target/scala-2.11/sp500_2.11-1.0.jar  /home/centos/SP500.csv 90

## Test Cases
1. Checks Sample Input file for expected Output
2. Checks 2nd Sample Input file for expected Output
3. Check invalid probability value
4. Check process function with false invalidated arg
5. Check invalid CSV file path

## Further Improvements:
As of now the project expects the file to be passed as local file.
-Handling HDFS input files

