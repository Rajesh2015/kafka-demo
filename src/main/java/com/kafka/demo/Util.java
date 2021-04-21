package com.kafka.demo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public enum Util {

    INSTNACE;

    String getBrokerConfig(String name){
        Config rootConfig = ConfigFactory.load("application.conf").getConfig("conf");
        Config brokerConfig = rootConfig.getConfig("broker");
       return brokerConfig.getString(name);


    }
    String getSchemaRegistryConfig(){
        Config rootConfig = ConfigFactory.load("application.conf").getConfig("conf");
        Config schemaRegistryConfig = rootConfig.getConfig("schemaregistry");
        return schemaRegistryConfig.getString("url");


    }
}
