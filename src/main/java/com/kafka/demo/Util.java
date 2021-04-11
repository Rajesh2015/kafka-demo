package com.kafka.demo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public enum Util {

    INSTNACE;

    String getConfig(String name){
        Config rootConfig = ConfigFactory.load("application.conf").getConfig("conf");
        Config brokerConfig = rootConfig.getConfig("broker");
       return brokerConfig.getString(name);


    }
}
