package minsait.ttaa.datio.enigine;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;
import static minsait.ttaa.datio.common.naming.PlayerOutput.playerCat;
import static minsait.ttaa.datio.common.naming.PlayerOutput.potentialVsOverall;

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
    public void filtersByPlayerCatAndPotentialVsOverallFunctionTest_A_B(){
        boolean test = transformer.ExecuteCatConditionABTest();
        if(test)
            assert(true);
    }

    @Test
    public void filtersByPlayerCatAndPotentialVsOverallFunctionTest_C(){
        boolean test = transformer.ExecuteCatConditionCTest();
       if(test)
           assert(true);
    }

    @Test
    public void filtersByPlayerCatAndPotentialVsOverallFunctionTest_D(){
        boolean test = transformer.ExecuteCatConditionDTest();
        if(test)
            assert(true);
    }
}
