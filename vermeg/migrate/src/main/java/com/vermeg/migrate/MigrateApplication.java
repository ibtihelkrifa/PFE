package com.vermeg.migrate;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.annotation.PostConstruct;

@SpringBootApplication
@EnableJpaRepositories
@EnableTransactionManagement
public class MigrateApplication {
	private static final Logger log = Logger.getRootLogger();

	@Autowired
	private SparkProperties properties;

	@PostConstruct
	public void init() {

		log.info("SparkPi submit jar is: "+properties.getJarFile());
		if (!SparkContextProvider.init(properties)) {
			// masterURL probably not set,
			// meaning this was likely run outside of oshinko
			System.exit(1);
		}
	}


	public static void main(String[] args) {

		SpringApplication.run(MigrateApplication.class, args);
	}

}
