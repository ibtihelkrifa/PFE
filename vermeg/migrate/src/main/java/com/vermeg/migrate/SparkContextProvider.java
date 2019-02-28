package com.vermeg.migrate;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextProvider {
    private static SparkContextProvider INSTANCE = null;

    private SparkConf sparkConf;
    private JavaSparkContext sparkContext;

    private SparkContextProvider() {
    }

    private SparkContextProvider(SparkProperties props) {
        this.sparkConf = new SparkConf().setAppName("JavaSpark").setMaster("local").set("spark.executor.memory","1g");
        this.sparkConf.setJars(new String[]{props.getJarFile()});
        this.sparkConf.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/betbet.test");
        this.sparkConf.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/betbet.test");
        this.sparkContext = new JavaSparkContext(sparkConf);

    }

    public static boolean init(SparkProperties props) {
        try {
            if (INSTANCE == null) {
                INSTANCE = new SparkContextProvider(props);
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    public static JavaSparkContext getContext() {
        return INSTANCE.sparkContext;
    }
}
