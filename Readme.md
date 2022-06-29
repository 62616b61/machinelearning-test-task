Please build a data pipeline that will:
* Read conversations data from `clean-data.json`
* Calculate the following metrics for each message:
	* `word_count`
	* `tone` - if the first word of the message has an odd number of letters, `tone` is negative, otherwise it's `positive`. Klaus' data scientists keep tweaking this algorithm to make it even more precise so this may change in the future.
* Calculate the following metrics for each conversation:
	* `total_word_count`
	* `min_message_word_count`
	* `max_message_word_count`
	* `average_message_word_count`
	* `tone` - the `tone` of the most messages in this conversation. If `positive` and `negative` messages are equal, set the conversation's `tone` to `mixed`
* Write the results into a file `processed-data.json` that has the same structure as the input file with the addition of the newly calculated fields.
* Anything that you can do to structure the pipeline in a way that makes it easier to add similar metrics in the future would be good.
* The starting point of an Apache Beam pipeline is provided but you can use any tool or framework you'd like.

Bonus:
* Create tests for the pipeline / the processing. Testing framework is up to you.