# OddKeyValuesFinder

A directory in S3 contains files with two columns

  * The files contain headers, the column names are just random strings and they are not consistent across files
  * both columns are integer values
  * Sometimes the files are CSV sometimes they are TSV
  * The empty string should represent 0
  * Henceforth the first column will be referred to as the key, and the second column the value
  * For any given key across the entire dataset (so all the files), there exists exactly one value that occurs an odd number of times.

Write an app where
  * The first column contains each key exactly once
  * The second column contains the integer occurring an odd number of times for the key
   
