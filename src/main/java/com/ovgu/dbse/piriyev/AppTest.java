package com.ovgu.dbse.piriyev;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import static core.algo.vertical.AbstractAlgorithm.Algo.AUTOPART;

import java.util.HashSet;
import java.util.Set;

import core.algo.vertical.*;
import db.schema.BenchmarkTables;
import experiments.AlgorithmResults;
import experiments.AlgorithmRunner;
/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
//    public AppTest( String testName )
//    {
//      //  super( testName );
//    }
//
//    /**
//     * @return the suite of tests being tested
//     */
//    public static Test suite()
//    {
//       // return new TestSuite( AppTest.class );
//    }
//
//    /**
//     * Rigourous Test :-)
//     */
//    public void testApp()
//    {
//      //  assertTrue( true );
//    }
    
//    public static void main(String [] args) {
//    	String[] queries = {"A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10"};
//    	Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
//    	AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = {AUTOPART};
//        for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
//            algos_sel.add(algo);
//        }
//        AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, queries, new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
//        algoRunner.runTPC_H_All();
//        String output = AlgorithmResults.exportResults(algoRunner.results);
//        System.out.println(output);
//    }
}
