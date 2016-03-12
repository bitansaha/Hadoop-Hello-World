#Total Sort#   #TotalOrderPartitioner#

###############################################################################################################################################################################

InputSampler performs the sampling during the Map stage (prior to both shuffle and reduce) and the sampling is done over the Mapper's Input KEY. We need to make sure the Input and Output KEY's of the mapper are the same; otherwise the MR framework won't find a suitable bucket to put the output Key, Value pairs with in the sampled space.

In this case the input KEY is LongWritable, and hence the InputSampler would create a partition based on a sub set of all LongWritable KEY's. But the output KEY is Text, hence the MR framework will fail to find a suitable bucket from with in the partition.

(To counter the problem we have introduced two jobs. The first one is a preparation job which transforms the text file <KEY -> LongWritable, VALUE -> Text> to a sequence file <KEY -> Text, VALUE -> IntWritable> to match the input as required by the sampler.)

																		/

(This is also the only reason why we had a Preparation Stage earlier because otherwise the input <KEY, VALUE> would have been <LongWritable, Text> (representing a file row read) and the output <KEY, VALUE> would have been <IntWritable, Text>, and hence the sampling won't match the output KEY.)
  