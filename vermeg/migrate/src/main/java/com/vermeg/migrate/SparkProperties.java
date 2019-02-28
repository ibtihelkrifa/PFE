package com.vermeg.migrate;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SparkProperties {

    @Value("${sparkpi.jarfile}")
    private String jarFile;

    public String getJarFile() {
        return jarFile;
    }
}
