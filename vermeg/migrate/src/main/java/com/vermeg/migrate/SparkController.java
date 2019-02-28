package com.vermeg.migrate;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
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

    @GetMapping("/sparkpi")
    public String sparkpi() {
        return sparkProducer.migrate();



    }
}