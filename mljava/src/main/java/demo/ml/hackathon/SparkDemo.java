package demo.ml.hackathon;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by karthick.s on 06-07-2018.
 */
public class SparkDemo {
    public static void main(String[] args) {

        String movieFile  = "mljava/datasets/movies.csv";

        SparkSession spark = new SparkSession.Builder()
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "temp/")
                .appName("ML Demo")
                .getOrCreate();

        // Read MovieFile
        Dataset<Row> moviesDF = spark.read()
                .format("com.databricks.spark.csv")
                .option("inferSchemea", "true")
                .option("header", "true")
                .load(movieFile);

        Dataset<Row> moviesSelectDF = moviesDF.select(moviesDF.col("movieId"), moviesDF.col("title"),
                moviesDF.col("genres"));

        moviesDF.show(10,false);

    }
}
