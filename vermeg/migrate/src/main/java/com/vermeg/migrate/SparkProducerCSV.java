package com.vermeg.migrate;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.bson.Document;
import org.springframework.stereotype.Service;

import javax.sound.sampled.Line;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

@Service
public class SparkProducerCSV implements Serializable {

    public String migratefromcsv()
    {

        JavaSparkContext jsc = SparkContextProvider.getContext();
        try{
        JavaRDD<String[]> liststring=jsc.textFile("./../../Desktop/test.csv").filter(line -> !line.startsWith("Name"))
                .map(line->line.split(","));



       /* JavaRDD<Document> rd= jsc.parallelize(liststring.collect()).map(new Function<String, Document>() {
            @Override
            public Document call(String line) throws Exception {
                return Document.parse("{name: "+line+"}");
            }
        });*/

        /*List<Document> ld= rd.collect();
List<String> ls= new ArrayList<String>();
            for (Document d:ld
                 ) {
                ls.add(d.get("name").toString());

            }*/




           /* Properties p= new Properties();
            p.setProperty("spark.mongodb.input.uri","mongodb://127.0.0.1/bet.test5");
            p.setProperty("spark.mongodb.output.uri","mongodb://127.0.0.1/bet.test5");
            p.setProperty("collection","test5");

            jsc.sc().setLocalProperties(p);*/
           // MongoSpark.save(rd);
           return "succes";
        }

         catch (Exception e)
         {
             return  e.getMessage();
         }
    }
}
