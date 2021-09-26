package cs523.finalPackage.consumer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

import cs523.finalPackage.config.Constants;

public class ConsumerMain {
	public static void main(String[] args) throws StreamingQueryException {

		Logger.getLogger("org").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder()
				.appName("Spark Kafka Integration Structured Streaming")
				.master("local[*]").getOrCreate();
		
		String[] columns = new String[] {
			"createdAt",
			"username",
			"screenName",
			"followersCount",
			"friendsCount",
			"userFavoritesCount",
			"location",
			"retweetsCount",
			"favoritesCount",
			"language",
			"suffix"
		};
		

		Dataset<Row> dataAsSchema = getSchemaFromColumns(spark, columns);
		StreamingQuery query = dataAsSchema.writeStream().outputMode("append").format("console").start();

		dataAsSchema.coalesce(1).writeStream()
		    .format("csv")        // can be "orc", "json", "csv", "parquet" etc.
		    .outputMode("append")
		    .trigger(Trigger.ProcessingTime(10))
	        .option("truncate", false)
	        .option("maxRecordsPerFile", 10000)
		    .option("path", "hdfs://localhost:8020/user/cloudera/twitterImport")
		    .option("checkpointLocation", "hdfs://localhost:8020/user/cloudera/twitterCheckpoint") //args[1].toString()
		    .start()
		    .awaitTermination();
		
		query.awaitTermination();

	}
	
	
	public static Dataset<Row> getSchemaFromColumns(SparkSession spark, String[] columns) {
		Dataset<Row> ds = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", Constants.KAFKA_TOPIC_NAME)
				.load();

		Dataset<Row> lines = ds.selectExpr("CAST(value AS STRING)");
		
		Dataset<Row> dataAsSchema = lines
                .selectExpr("value",
                        "split(value,',')[0] as createdAt",
                        "split(value,',')[1] as username",
                        "split(value,',')[2] as screenName",
                        "split(value,',')[3] as followersCount",
                        "split(value,',')[4] as friendsCount",
                        "split(value,',')[5] as userFavoritesCount",
                        "split(value,',')[6] as location",
                        "split(value,',')[7] as retweetsCount",
                        "split(value,',')[8] as favoritesCount",
                        "split(value,',')[9] as language",
                        "split(value,',')[10] as suffix")
                .drop("value");
		
		for (String column: columns) {
			dataAsSchema = dataAsSchema.withColumn(column, functions.regexp_replace(functions.col(column), " ", ""));
		}
		
		for (String column: columns) {
			dataAsSchema = dataAsSchema.withColumn(column, functions.col(column).cast(DataTypes.StringType));
		}
		
		return dataAsSchema;
	}

}
