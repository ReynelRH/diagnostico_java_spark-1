package minsait.ttaa.datio.engine;

import minsait.ttaa.datio.common.Common;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;
    private Properties prop;

    public Transformer(@NotNull SparkSession spark) {
        loadProperties();
        this.spark = spark;
    }

    public void ExecuteMethods(){
        Dataset<Row> df = readInput();
        df.printSchema();
        df = cleanData(df);
        df = filterAgePlayersFunction(df);
        df = playerCatFunction(df);
        df = potentialVsOverallFunction(df);
        df = columnSelection(df);
        df = filtersByPlayerCatAndPotentialVsOverallFunction(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
        //write(df, prop.getProperty("pathOut") + prop.get("outputDataFileName"));
    }

    /**
     *
     * @param df is a Dataset with players information.
     * @return a Dataset readed from csv file.
     */
    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                playerCat.column(),
                potentialVsOverall.column()
        );
    }

    /**
     * @return a Dataset readed from csv file.
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(prop.getProperty("pathIn") + prop.getProperty("inputFileName"));
        return df;
    }

    /**
     * @param df is a Dataset with players information.
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest

    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }
    */

    /**
     * Read paths for input and output files from properties file.
     */
    private void loadProperties(){
        prop =  new Properties();
        try {
            prop.load(new FileInputStream(NAME_FILE_PROPERTIES));
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    /**
     *
     * @param df is a Dataset with players information.
     * @return Dataset filtered by age of players, depends of the value of parameter from properties file.
     */
    private Dataset<Row> filterAgePlayersFunction(Dataset<Row> df) {
        if(prop.getProperty("runProcessType").equals("1"))
            df = df.filter(
                    age.column().isNotNull().and(
                            age.column().$less(23)
                    )
            );
        return df;
    }

    /**
     * @param df is a Dataset with players information (must have nationality  and team_position  columns)
     * @return add to the Dataset the column "player_cat"
     * by each position value
     * cat A for if the player is one of the best 3 players in his position in his country.
     * cat B for if the player is one of the best 5 players in his position in his country.
     * cat C for if the player is one of the best 10 players in his position in his country.
     * cat D for the rest
     */
    private Dataset<Row> playerCatFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column())
                .partitionBy(teamPosition.column())
                .orderBy(overall.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(3), "A")
                .when(rank.$less(5), "B")
                .when(rank.$less(10), "C")
                .otherwise("D");

        df = df.withColumn(playerCat.getName(), rule);

        return df;
    }

    /**
     *
     * @param df
     * @return add to the Dataset the column "potential_vs_overall"
     */
    private Dataset<Row> potentialVsOverallFunction(Dataset<Row> df) {
        Column newColumn = df.col(potential.getName()).divide(df.col(overall.getName()));
        df = df.withColumn(potentialVsOverall.getName(), newColumn);
        return df;
    }

    /**
     *
     * @param df
     * @return dataset filter according to the player_cat and potential_vs_overall columns with the following conditions:
     * If player_cat is in the following values: A, B
     * If player_cat is C and potential_vs_overall is greater than 1.15
     * If player_cat is D and potential_vs_overall is greater than 1.25
     */
    private Dataset<Row> filtersByPlayerCatAndPotentialVsOverallFunction(Dataset<Row> df) {
        df = df.filter(
                playerCat.column().isNotNull().and(
                        playerCat.column().equalTo("A").or(playerCat.column().equalTo("B"))
                                .or(playerCat.column().equalTo("C").and(potentialVsOverall.column().$greater(1.15)))
                                .or(playerCat.column().equalTo("D").and(potentialVsOverall.column().$greater(1.25)))
                )
        );
        return df;
    }
}
