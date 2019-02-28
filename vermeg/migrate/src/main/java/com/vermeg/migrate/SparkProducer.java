package com.vermeg.migrate;


import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.tomcat.util.json.JSONParser;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.util.parsing.json.JSONObject;

import static java.util.Arrays.asList;

import java.io.Serializable;
import java.util.*;

@Service
public class SparkProducer implements Serializable {





    public String GetPi(int scale) {
            JavaSparkContext jsc = SparkContextProvider.getContext();
            String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
            String MYSQL_USERNAME = "root";
            String MYSQL_PWD = "root";
            String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/migrate?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
            SQLContext sqlContext = new SQLContext(jsc);
            DOMConfigurator.configure("src/main/resources/log4j.properties");
            Map<String, String> options = new HashMap<>();
            options.put("driver", MYSQL_DRIVER);
            options.put("url", MYSQL_CONNECTION_URL);
            options.put("dbtable", "customers");
            DataFrame jdbcDF = sqlContext.load("jdbc", options);
            MongoSpark.write(jdbcDF).option("collection", "test").mode("overwrite").save();
            List<Row> customers = jdbcDF.collectAsList();

        return customers.toString();

        }
    }

