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
        //SparkConf sparkConf= jsc.getConf();
     //   sparkConf.set();
       // sparkConf.set();


        SQLContext sqlContext = new SQLContext(jsc);

        Properties properties= new Properties();
            properties.put("user","root");
            properties.put("password","root");
            DataFrame jdbcDF=sqlContext.read().jdbc(MYSQL_CONNECTION_URL,"customers",properties);
            DataFrame df=sqlContext.read().jdbc(MYSQL_CONNECTION_URL,"product",properties);
            df.registerTempTable("product");
             jdbcDF.registerTempTable("customer");
             DataFrame c= sqlContext.sql("select c.id, concat(c.namec,' ',c.lastnamec),p.namep from customer c, product p where c.product_id=p.id");
             DataFrame c1=c.toDF("id","FullName","Product_name");
             //MongoSpark.write(c1).option("collection", "test8").mode("overwrite").save();
             List<Row> customers = c1.collectAsList();
               MongoSpark.write(c1).option("spark.mongodb.input.uri","mongodb://127.0.0.1/bet")
               .option("spark.mongodb.output.uri","mongodb://127.0.0.1/bet")
               .option("collection","test1").mode("overwrite").save();
     /*   Map<String, String> writeOverrides = new HashMap<String, String>();

        writeOverrides.put("spark.mongodb.input.uri","mongodb://127.0.0.1/bet");
        writeOverrides.put("spark.mongodb.output.uri","mongodb://127.0.0.1/bet");
        writeOverrides.put("collection","coll");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
        MongoSpark.save(c1,writeConfig);*/
        return customers.toString();

        }
    }

