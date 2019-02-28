package com.vermeg.migrate;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class SparkController {

    @Autowired
    SparkProducer sparkProducer;

    @RequestMapping("/")
    public String index() {
        return "Java Spring Boot Spark server running. Add the 'sparkpi' route to this URL to invoke the app.";
    }

    @RequestMapping("/sparkpi")
    public String sparkpi(@RequestParam(value="scale", defaultValue="2") String scale) {
        return sparkProducer.GetPi(2);



    }
}