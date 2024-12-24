## Tasks

1. Calculate Daily Active Users (DAU) and Monthly Active Users (MAU) for the past year:
   - Define clear criteria for what constitutes an "active" user
   > This is a subjective criteria and depends on the product’s core value proposition and what constitutes meaningful engagement.
   > 1.	Minimal Engagement
	 > A user is “active” if they have performed at least one relevant action (e.g., creating a note) in the past 24 hours.
	 > 2. Core Feature Usage
	 > A user is “active” if they have triggered a “core action” (created a new note and viewed it) at least twice in the past 7 days.
	 > 3. Session-Based Criteria
	 > A user is “active” if they have initiated at least one session in the last 30 days and spent more than 2 minutes in that session.

   - Explain how you handle outliers and extremely long duration values
   > 	Outliers and anomalies can be detected via statistical methods (e.g. Z-Score, Percentile Capping) and Logical Thresholds. Once we've identified the outliers or "bad data", we can handle them by:
   > 1.	Filtering Out:
	 >	If extremely long sessions are likely erroneous (e.g., user left the application open overnight), you can exclude them from aggregations like average session duration.
	 > 2.	Capping The Values:
	 > Instead of discarding data, cap outlier values at a certain percentile or a domain-based maximum. For instance, any session longer than 10 hours is set to 10 hours.
	 > 3.	Separate Buckets or Flags:
	 > Keep outliers in your dataset but treat them differently in reporting:
	 > Bucket them in a separate category (e.g., “Session > 8 hours”).
	 > Flag them with a binary column (e.g., is_outlier = True).

2. Calculate session-based metrics:
   - Define clear criteria for what constitutes a "session"
   > A session is typically defined as a continuous period of user activity bounded by a chosen inactivity threshold. First, we sort each user’s events by time and compute the time gap to the previous event. If this gap is greater than or equal to your threshold (e.g., 30 minutes) or if it’s the user’s first event, we flag a new session. By taking a cumulative sum of those flags in time order, each event is assigned to a unique session ID. This effectively groups consecutive events (with gaps below the threshold) into a single session, allowing us to measure session start/end times, durations, and the number of actions within each session. 

3. Spark UI Analysis
<img width="1372" alt="Screenshot 2024-12-24 at 01 45 09" src="https://github.com/user-attachments/assets/f18f23d5-dd74-4251-b993-9505c792f04e" />

   
4. Data Processing and Optimization

   a. In the above Spark job, join the User Interactions and User Metadata datasets efficiently while handling data skew:
      - Explain your approach to mitigating skew in the join operation, considering that certain user_ids may be significantly more frequent
      > Here's the approach i would take:
      >  -	Manual Salting: Most explicit, highly effective for extremely large skew, but requires duplicating records on both sides of the join.
      >  -	Broadcast Joins: Easiest if one dataset is small enough.
      >  -	Skew Hints: Let Spark handle it with minimal code changes.
      >  -	Partition Management: Repartition by user_id to spread out tasks, but may not suffice if one user_id dwarfs all others.
    
      > Mixing and matching the above strategies, we can mitigate join skew and achieve better performance and resource usage. 

      - Implement and justify your choice of join strategy (e.g., broadcast join, shuffle hash join, sort merge join)
      > Since we are using Spark 3+ i would use the SKEW hints to let SPARK handle the skews. If one dataframe is obviously small i would implement broadcast joins.

5. Design and implement optimizations for the Spark job to handle large-scale data processing efficiently:

   2. Memory Management and Configuration:
      - Design a memory tuning strategy for executors and drivers
      > I recommend paying close attention to memory management in Spark by properly sizing both the driver and executor memory, as well as tuning how Spark divides that memory between storage (for caching) and execution (for shuffles and joins). I make sure to allocate enough memory so that my jobs don’t constantly garbage collect or run out of memory, but not so much that each executor becomes cumbersome to manage. I also optimize by partitioning my data thoughtfully, broadcasting smaller tables to reduce big shuffles, and caching data only when necessary—then unpersisting it once I’m done. I keep an eye on the Spark UI, particularly the Executors and Storage tabs, to see how memory is being used, where garbage collection time is high, or whether disk spills are happening. With these insights, I adjust settings like spark.executor.memory, spark.memory.fraction, and the GC algorithm (often G1 GC) to maintain high performance without overwhelming my cluster resources.
      
      - Document memory allocation for larger data scales
      > -	For 100GB scale, a relatively modest cluster (e.g., 5 nodes with 64GB each) can suffice using 8–12GB executors, provided you’re not caching the entire dataset.
    	> -	At 500GB, you might scale to ~10 nodes, 40 executors, each with around 12GB+ of heap and 2–4GB overhead.
    	> -	At 1TB, you might go to 20 or more nodes, each running multiple executors with ~16GB heap, ensuring you have enough parallelism (thousands of shuffle partitions) for big transformations.
    	> -	In all cases, monitor the Spark UI, watch for memory spills, and fine-tune your partitioning and overhead settings based on real-time job performance.

  3. Parallelism and Resource Utilization:
      - Determine optimal parallelism based on data volume and cluster resources
      - Implement dynamic partition tuning
      > Enable by setting spark.sql.adaptive.enabled=true and related configs (spark.sql.adaptive.shuffle.reducePostShufflePartitions.enabled=true).
      - Provide configuration recommendations for:
         - Number of executors
         - Cores per executor
         - Memory settings
         - Shuffle partitions
  
      > Number of Executors
    	>  1.	Node Capacity:
    	>  -	If a node has 64 GB RAM and 16 cores, you might launch 4 executors each with 4 cores (giving 4 executors × 4 cores = 16 total cores per node).
    	>  -	This ensures all cores are used without having a single massive executor.
    	>  2.	Cluster-Wide:
    	>  -	Multiply executors per node by the number of nodes.
    	>  -	Example: 5 nodes × 4 executors per node = 20 total executors.
    
      > Cores per Executor
    	> - 2 to 5 Cores per executor is a good balance.
    	> - Fewer cores per executor often helps with shorter GC pauses and simpler scheduling.
    	> - More cores can be beneficial if tasks are CPU-bound, but large heaps can hurt GC performance.
    
      > Memory Settings
    	> 1.	Executor Memory
      > -	Common range: 8–16GB per executor for moderate data volumes.
    	> -	Too large heaps (>32 GB) can cause long GC pauses. Scale out (more executors) rather than up.
      >	2.	Memory Overhead
      >	-	Set spark.executor.memoryOverhead to 10–25% of executor heap.
      >	-	This overhead covers JVM metaspace, Python processes, and off-heap structures.
      >	3.	Driver Memory
      >	-	Sufficient for job planning and broadcast variables. If the driver also loads data (e.g., collect actions), consider 2–4GB or more depending on job scale.
      
      > Shuffle Partitions
      >	1.	Base Partitions
      >	-	Set spark.sql.shuffle.partitions so that total shuffle partitions are roughly 2–4× total cores.
      >	-	For a 160-core cluster, you might start with 320–640 shuffle partitions.
      >	2.	Adaptive Execution
      >	-	Let Spark further reduce or merge partitions post-shuffle. This can drastically improve efficiency if the data is smaller than expected. 
      >	3.	Large Data Considerations
      >	-	For 1 TB+ workloads, you might increase the shuffle partitions to 2000+ to keep tasks smaller.
      >	-	Always watch for task skew (some tasks taking much longer), which might require salting or advanced partition strategies.
            
6. Data Quality + Error Handling
   > Data Quality: This is a very large topic. Typically I would build checks for Schema Validation, Typing Correctness, Null Values, Duplicates, Are values in logical range etc.
   > Error Handling: Spark transformations should be wrapped in try/except blocks to capture exceptions. Where there is I/O interaction a retry strategy should be implemented.
   > Checkpoints: The Spark pipelines should be checkpoints so that one can recover and resume from last good state.

7. Production Deployment Setup
   
![Untitled-2024-12-24-0338-2](https://github.com/user-attachments/assets/48c4ff49-2d00-4135-9c02-ce1500b72f86)

  To deploy a Spark data pipeline in production:
  1.	Pick a production environment (e.g., AWS EMR, Databricks, DataProc, Kubernetes etc).
  2.	Package your code and dependencies in a robust manner (I use [UV](https://docs.astral.sh/uv/) to pin the dependencies deterministically).
  3.	Orchestrate the pipeline using a scheduler (Airflow, Databricks Jobs, Dagster etc.).
  4.	Implement CI/CD so code changes are tested, versioned, and automatically deployed. e.g. GitHub Actions
  5.	Maintain logging & monitoring (Spark UI, logs, metrics) to keep track of job performance and failures.
  6.	Integrate data quality checks and error handling to ensure reliable and trustworthy analytics.
