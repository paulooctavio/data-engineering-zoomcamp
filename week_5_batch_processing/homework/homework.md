## Week 5 Homework

In this homework we'll put what we learned about Spark
in practice.

We'll use high volume for-hire vehicles (HVFHV) dataset for that.

## Question 1. Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session 
* Execute `spark.version`

What's the output?

**Answer:** '3.0.3'

## Question 2. HVFHW February 2021

Download the HVFHV data for february 2021:

```bash
wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-02.csv
```

Read it with Spark using the same schema as we did 
in the lessons. We will use this dataset for all
the remaining questions.

Repartition it to 24 partitions and save it to
parquet.

What's the size of the folder with results (in MB)?

**Answer:** 219b

## Question 3. Count records 

How many taxi trips were there on February 15?

Consider only trips that started on February 15.

**Answer:** 367170

## Question 4. Longest trip for each day

Now calculate the duration for each trip.

Trip starting on which day was the longest? 

**Answer:** 2021-02-11

## Question 5. Most frequent `dispatching_base_num`

Now find the most frequently occurring `dispatching_base_num` 
in this dataset.

How many stages this spark job has?

> Note: the answer may depend on how you write the query,
> so there are multiple correct answers. 
> Select the one you have.

**Answer:** 3 stages

## Question 6. Most common locations pair

Find the most common pickup-dropoff pair. 

For example:

"Jamaica Bay / Clinton East"

Enter two zone names separated by a slash

If any of the zone names are unknown (missing), use "Unknown". For example, "Unknown / Clinton East". 

**Answer:** Crown Heights North / Crown Heights North

## Bonus question. Join type

(not graded) 

For finding the answer to Q6, you'll need to perform a join.

What type of join is it? **Answer:** Broadcasting

And how many stages your spark job has? **Answer:** 1
