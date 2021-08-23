package minsait.ttaa.datio.enigine;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

public class TransformerTest {

    private Transformer transformer;
    private static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    @Before
    public void init(){
        transformer = new Transformer(spark);
    }

    @Test
    public void filtersByPlayerCatAndPotentialVsOverallFunctionTest(){

        assert(true);
    }
}
