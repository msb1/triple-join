<h4><strong>Triple Joins with Simulated Data</strong></h4>
<ul>
<li>Cards are generated with id and additional data</li>
<li>Verifications by Users are generated</li>
<li>User are generated</li>
<li>First verifications are joined to cards</li>
<li>Next, users are joined to the verified cards</li>
<li>Then an aggregation is performed on the users where the cards verified by each user are determined</li>
<li>Finally a filter is applied to only output users with greater than a certain number (200) of verifications</li>
</ul>
<h4><strong>Case 1: SQL with SQLite3 in Python (no ORM)</strong></h4>
<h4><strong>Case 2: Pandas in Python with Dataframes</strong></h4>
<h4><strong>Case 3: Scala Spark Structured Streaming with Kafka generated streams</strong></h4>
<ol>
<li>Program 1 generates three simulated data streams to three Kafka Producer topics</li>
<li>Program 2 performs the Triple Join with Aggregation</li>
</ol>
<h4><strong>Case 4: Scala Kafka Streams</strong></h4>
<ol>
<li>Use same Program 1 from Case 3 to generate data records to Producer topics</li>
<li>Program is Scala Kafka Streams implementation of Triple Join with aggregation</li>
</ol>
