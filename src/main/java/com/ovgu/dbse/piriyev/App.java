package com.ovgu.dbse.piriyev;

import com.ovgu.dbse.piriyev.resources.PartitionResource;
import io.dropwizard.Application;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App extends Application<PartitionConfiguration>
{
    public static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    @Override
    public void initialize(Bootstrap<PartitionConfiguration> b) {}

    @Override
    public void run(PartitionConfiguration configuration, Environment environment) throws Exception {
        //environment.jersey().register(new ContactResource());
        environment.jersey().register(new PartitionResource());
    }

    public static void main( String[] args ) throws Exception {
        new App().run(args);
    }
}
