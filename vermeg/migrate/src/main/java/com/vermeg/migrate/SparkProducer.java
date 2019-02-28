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
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.tomcat.util.json.JSONParser;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.util.parsing.json.JSONObject;

import javax.xml.validation.Schema;

import static java.util.Arrays.asList;

import java.io.Serializable;
import java.util.*;

@Service
public class SparkProducer implements Serializable {





    public String migrate() {
            JavaSparkContext jsc = SparkContextProvider.getContext();

        String MYSQL_CONNECTION_URL="jdbc:mysql://localhost:3306/migrate";
        SQLContext sqlContext = new SQLContext(jsc);

            Properties properties= new Properties();
            properties.put("user","root");
            properties.put("password","root");
            DataFrame jdbcDF=sqlContext.read().jdbc(MYSQL_CONNECTION_URL,"customers",properties);
             jdbcDF.registerTempTable("customer");
             DataFrame c= sqlContext.sql("select id, concat(namec,' ',lastnamec),product_id from customer");
             DataFrame c1=c.toDF("id","FullName","Product_id");
             MongoSpark.write(c1).option("collection", "test4").mode("overwrite").save();
             List<Row> customers = c1.collectAsList();
             return customers.toString();

        }
    }

