package experiments;

import core.algo.vertical.*;
import core.config.DataConfig;
import core.utils.ArrayUtils;
import db.schema.BenchmarkTables;
import db.schema.BenchmarkTables.BenchmarkConfig;
import db.schema.entity.Attribute;
import db.schema.entity.Table;
import db.schema.entity.Workload;
import db.schema.types.AttributeType;
import db.schema.types.TableType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static core.algo.vertical.AbstractAlgorithm.Algo.*;

/**
 * Class for running the algorithms on different tables for a specified cost model.
 *
 * @author Endre Palatinus
 */
public class AlgorithmRunner {

    public static final String[] allTPCHQueries = new String[22];
    static {
        for (int i=1; i<=22; i++) { allTPCHQueries[i-1] = "Q" + i;}
    };

    public String[] querySet;
    

    public static final int replicationFactor = 1;
    public static final double THRESHOLD = 0.0;
    public static final double LINE_ITEM_THRESHOLD = 0.25;

    protected static boolean RUN_TROJAN = true;
    protected static boolean RUN_NAVATHE = true;
    protected static boolean RUN_OPTIMAL = true;

    public static final double[] generalCGrpThreshold = new double[]{THRESHOLD/*, THRESHOLD, THRESHOLD*/};
    public static final double[] lineitemCGrpThreshold = new double[]{LINE_ITEM_THRESHOLD/*, LINE_ITEM_THRESHOLD, LINE_ITEM_THRESHOLD*/};

    protected DreamPartitioner dreamPartitioner;
    protected AutoPart autoPart;
    protected HillClimb hillClimb;
    protected HillClimbCL hillClimbCL;
    protected HYRISE hyrise;
    protected NavatheAlgorithm navatheAlgo;
    protected O2P o2p;
    protected TrojanLayout trojanLayout;
    protected Optimal optimal;

    protected double[] runTimes;

    protected AbstractAlgorithm.AlgorithmConfig config;
    protected BenchmarkTables.BenchmarkConfig benchmarkConf;

    /** The list of algorithms to run. */
    protected Set<AbstractAlgorithm.Algo> algos;
    /** The default list of algorithms to run. */
    public static final AbstractAlgorithm.Algo[] ALL_ALGOS = {AUTOPART, HILLCLIMB, HYRISE, NAVATHE, O2P, TROJAN,
            OPTIMAL, DREAM, COLUMN, ROW};

    /* JVM and testing specific parameters. */

    /** Iterations performed initially just to allow for the JVM to optimize the code.  */
    protected static final int JVM_HEAT_UP_ITERATIONS = 5;
    /** Number of times a single experiment is repeated. */
    protected static final int REPETITIONS = 5;
    /** The limit on the runtime of the algorithms for which we perform only one run.  */
    public static final double NO_REPEAT_TIME_LIMIT = 30;

    public AlgorithmResults results;

    /**
     * Default constructor which creates a set-up for running all algorithms, scale factor 10 and HDD cost model.
     */
    public AlgorithmRunner() {
        this(null, 10.0, new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
    }

    /**
     * Constructor which creates a set-up for running all queries.
     *
     * @param algos The list of algorithms to run.
     * @param scaleFactor The scale factor of the benchmark dataset.
     * @param config The algorithm config including the cost calculator type used.
     */
    public AlgorithmRunner(Set<AbstractAlgorithm.Algo> algos, double scaleFactor,
                           AbstractAlgorithm.AlgorithmConfig config) {
        this(algos, scaleFactor, null, config);
    }

    /**
     * Constructor where you can specify every aspect of the experiment.
     *
     * @param algos The list of algorithms to run.
     * @param scaleFactor The scale factor of the benchmark dataset.
     * @param querySet The list of queries to run.
     * @param config The algorithm config including the cost calculator type used.
     */
    public AlgorithmRunner(Set<AbstractAlgorithm.Algo> algos, double scaleFactor,
                           String[] querySet, AbstractAlgorithm.AlgorithmConfig config) {

        this.config = config;

        benchmarkConf = new BenchmarkTables.BenchmarkConfig(null, scaleFactor, DataConfig.tableType);
        runTimes = new double[AbstractAlgorithm.Algo.values().length];
        results = new AlgorithmResults();

        if (algos == null) {
            this.algos = new HashSet<AbstractAlgorithm.Algo>();
            for (AbstractAlgorithm.Algo algo : AlgorithmRunner.ALL_ALGOS) {
                this.algos.add(algo);
            }
        } else {
            this.algos = algos;
        }

        this.querySet = querySet;
    }

    /**
     * Method for running the experiment for all TPC-H tables.
     */
    public void runTPC_H_Tables() {
        runTPC_H_Customer();
        runTPC_H_LineItem(true);
        runTPC_H_Orders();
        runTPC_H_Supplier();
        runTPC_H_Part();
        runTPC_H_PartSupp();
        runTPC_H_Nation();
        runTPC_H_Region();
    }

    /*Begin Debugging Begin*/
    public void runTPC_H_All() {

        RUN_TROJAN = false;
        RUN_OPTIMAL = false;
        RUN_NAVATHE = false;

        Table table = BenchmarkTables.tpchAll(benchmarkConf);
        config.setTable(table);
        runAlgorithms(config, lineitemCGrpThreshold);
        //runAlgorithms(config, generalCGrpThreshold);

        RUN_TROJAN = true;
        RUN_OPTIMAL = true;
        RUN_NAVATHE = true;

    }
    /*End Debugging End*/


    /**
     * Method for running the experiment for TPC-H Customer.
     */
    public void runTPC_H_Customer() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchCustomer(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H LineItem.
     */
    public void runTPC_H_LineItem(boolean runOptimal) {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchLineitem(benchmarkConf), null, querySet);

        config.setTable(table);
        RUN_OPTIMAL = runOptimal;
        runAlgorithms(config, lineitemCGrpThreshold);
        RUN_OPTIMAL = true;
    }

    /**
     * Method for running the experiment for TPC-H LineItem.
     */
    public void runTPC_H_LineItem(String[] queries) {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchLineitem(benchmarkConf), null, querySet);

        config.setTable(table);
        RUN_TROJAN = false;
        RUN_OPTIMAL = false;
        runAlgorithms(config, lineitemCGrpThreshold);
        RUN_TROJAN = true;
        RUN_OPTIMAL = true;
    }

    /**
     * Method for running the experiment for TPC-H Part.
     */
    public void runTPC_H_Part() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchPart(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H Part.
     */
    public void runTPC_H_Part(String[] queries) {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchPart(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H Supplier.
     */
    public void runTPC_H_Supplier() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchSupplier(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H PartSupp.
     */
    public void runTPC_H_PartSupp() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchPartSupp(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H Orders.
     */
    public void runTPC_H_Orders() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchOrders(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H Orders.
     */
    public void runTPC_H_Orders(String[] queries) {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchOrders(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H Nation.
     */
    public void runTPC_H_Nation() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchNation(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for TPC-H Region.
     */
    public void runTPC_H_Region() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchRegion(benchmarkConf), null, querySet);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**********************************/
    /************** SSB ***************/

    /**
     * Method for running the experiment for all SSB tables.
     * @param runOptimal Whether to run optimal for this table or not.
     */
    public void runSSB_Tables(boolean runOptimal) {
        runSSB_Customer();
        runSSB_Part();
        runSSB_Supplier();
        runSSB_Date();
        runSSB_LineOrder(runOptimal);
    }

    /**
     * Method for running the experiment for SSB Customer.
     */
    public void runSSB_Customer() {
        Table table = BenchmarkTables.ssbCustomer(benchmarkConf);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for SSB LineOrder.
     * @param runOptimal Whether to run optimal for this table or not.
     */
    public void runSSB_LineOrder(boolean runOptimal) {
        Table table = BenchmarkTables.ssbLineOrder(benchmarkConf);

        config.setTable(table);
        RUN_OPTIMAL = runOptimal;
        runAlgorithms(config, lineitemCGrpThreshold);
        RUN_OPTIMAL = true;
    }

    /**
     * Method for running the experiment for SSB Part.
     */
    public void runSSB_Part() {
        Table table = BenchmarkTables.ssbPart(benchmarkConf);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for SSB Supplier.
     */
    public void runSSB_Supplier() {
        Table table = BenchmarkTables.ssbSupplier(benchmarkConf);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**
     * Method for running the experiment for SSB Date.
     */
    public void runSSB_Date() {
        Table table = BenchmarkTables.ssbDate(benchmarkConf);

        config.setTable(table);
        runAlgorithms(config, generalCGrpThreshold);
    }

    /**********************************/

    /**
     *  Method for running all the algorithms on the same table with the specified cost model and workload.
     * @param config The config that all algorithms should use.
     * @param trojanLayoutThresholds The pruning thresholds for Trojan layout.
     */
    public void runAlgorithms(AbstractAlgorithm.AlgorithmConfig config, double[] trojanLayoutThresholds) {

        //create storage for the current table
        String tableName = config.getTable().name;
        results.addResultsForTable(tableName);

        dreamPartitioner = new DreamPartitioner(config);
        runAlgorithm(dreamPartitioner, tableName);

        autoPart = new AutoPart(config);
        autoPart.setReplicationFactor(0.0);
        runAlgorithm(autoPart, tableName);

        hillClimb = new HillClimb(config);
        runAlgorithm(hillClimb, tableName);

        hillClimbCL = new HillClimbCL(config);
        runAlgorithm(hillClimbCL, tableName);
        
        hyrise = new HYRISE(config);
        runAlgorithm(hyrise, tableName);

        navatheAlgo = new NavatheAlgorithm(config);
        if (RUN_NAVATHE) {
            runAlgorithm(navatheAlgo, tableName);
        }

        o2p = new O2P(config);
        runAlgorithm(o2p, tableName);

        optimal = new Optimal(config);
        if (RUN_OPTIMAL) {
            runAlgorithm(optimal, tableName);
        }

        if (RUN_TROJAN) {
            trojanLayout = new TrojanLayout(config);
            trojanLayout.setReplicationFactor(replicationFactor);
            trojanLayout.setPruningThresholds(trojanLayoutThresholds);

            // CAUTION: this is only correct for replication factor 1!
            trojanLayout = runTrojan(trojanLayout, config, trojanLayoutThresholds, tableName);
        }
    }

    private void runAlgorithm(AbstractAlgorithm algorithm, String tableName) {

        if (!algos.contains(algorithm.type)) {
            return;
        }

        runTimes[algorithm.type.ordinal()] = 0.0;

        for (int i = 0; i < JVM_HEAT_UP_ITERATIONS + REPETITIONS; i++) {
            algorithm.partition();

            if (algorithm.getTimeTaken() > NO_REPEAT_TIME_LIMIT) {
                runTimes[algorithm.type.ordinal()] = algorithm.getTimeTaken();
                results.storeResults(tableName, algorithm, runTimes[algorithm.type.ordinal()]);

                return;
            }

            if (i >= JVM_HEAT_UP_ITERATIONS) {
                runTimes[algorithm.type.ordinal()] += algorithm.getTimeTaken();
            }
        }

        runTimes[algorithm.type.ordinal()] /= REPETITIONS;
        results.storeResults(tableName, algorithm, runTimes[algorithm.type.ordinal()]);
    }

    private TrojanLayout runTrojan(TrojanLayout algorithm, AbstractAlgorithm.AlgorithmConfig config, double[] trojanLayoutThresholds, String tableName) {

        if (!algos.contains(algorithm.type)) {
            return null;
        }

        runTimes[algorithm.type.ordinal()] = 0.0;

        for (int i = 0; i < JVM_HEAT_UP_ITERATIONS + REPETITIONS; i++) {

            algorithm = new TrojanLayout(config);
            algorithm.setReplicationFactor(replicationFactor);
            algorithm.setPruningThresholds(trojanLayoutThresholds);
            algorithm.setPruningThreshold(THRESHOLD);

            algorithm.partition();

            if (algorithm.getTimeTaken() > NO_REPEAT_TIME_LIMIT) {
                runTimes[algorithm.type.ordinal()] = algorithm.getTimeTaken();
                results.storeResults(tableName, algorithm, runTimes[algorithm.type.ordinal()]);

                return algorithm;
            }

            if (i >= JVM_HEAT_UP_ITERATIONS) {
                runTimes[algorithm.type.ordinal()] += algorithm.getTimeTaken();
            }
        }

        runTimes[algorithm.type.ordinal()] /= REPETITIONS;
        results.storeResults(tableName, algorithm, runTimes[algorithm.type.ordinal()]);

        return algorithm;
    }

    /*Begin Debugging Begin*/
    public static void main(String[] args) {
    	/*Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
    	String[] queries = {"A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10"};
        AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = {AUTOPART, HILLCLIMB, HYRISE};
        for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
            algos_sel.add(algo);
        }
        AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, queries, new AbstractAlgorithm.HDDAlgorithmConfig(BenchmarkTables.randomTable(1, 1)));
        algoRunner.runTPC_H_All();
        String output = AlgorithmResults.exportResults(algoRunner.results);*/
//        AlgorithmRunner algorithmRunner = new AlgorithmRunner();
//        algorithmRunner.runTPC_H_All();
//        System.out.println(AlgorithmResults.exportResults(algorithmRunner.results));

        //System.out.println(output);
    	
    	
//    	//Define Table: List of attributes, names, primary key, table name, scale factor, number of rows
//    	List<Attribute> attr = new ArrayList<>();
//    	Attribute a = new Attribute("A", AttributeType.Integer());
//		a.primaryKey = true;
//		attr.add(a);
//		Attribute b = new Attribute("B", AttributeType.Boolean());
//		b.primaryKey = false;
//		attr.add(b);
//		Attribute c = new Attribute("C", AttributeType.Integer());
//		c.primaryKey = false;
//		attr.add(c);
//    	Table t = new Table("DummyTable", TableType.Default(), attr);
//
//    	
//    	//Define Queries (and assign to both table and algorithm runner): Names, and type of each query
//    	String[] queries = {"Q1","Q3"};
//    	Workload wkld = new Workload(attr, 60000, "DummyTable");
//    	wkld.addProjectionQuery("Q1", 1, 1);
//    	wkld.addProjectionQuery("Q3", 1, 2);
//    	t.workload = wkld; //I think that here we also get a chance of changing the cost model
//
//
//    	//Define cost model (and complete configuration for the algorithm)
//    	AbstractAlgorithm.HDDAlgorithmConfig conf = new AbstractAlgorithm.HDDAlgorithmConfig(t);
//    	//conf.setW(wkld.getSimplifiedWorkload());//Here we are choosing the cost model, implicitly
//
//    	//Define: Partitioning Algorithms
//    	Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
//    	AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = {AUTOPART, HILLCLIMB, O2P, DREAM};
//    	for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
//            algos_sel.add(algo);
//        }
//    	
//    	//DEfine algorithm runner (same for all)
//        AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, queries, conf);
//        
//        //Run
//        algoRunner.runAlgorithms(conf, null);
//
//        //algoRunner.runTPC_H_LineItem(true);
//        
//        //Get results....
//        AbstractAlgorithm.AlgorithmConfig config = algoRunner.getConfiguration();
//        
//        Table tab = config.getTable();
//        List<Attribute> attributes = tab.attributes;	
//        
//        for(Attribute attrib: attributes) {
//        	System.out.println(attrib.name);
//        }
//        
//        String output = AlgorithmResults.exportResults(algoRunner.results);
//        System.out.println(output);
    	
    	
      String[] queries = {"Q1","Q3","Q4","Q5"};
      Set<AbstractAlgorithm.Algo> algos_sel = new HashSet<AbstractAlgorithm.Algo>();
      AbstractAlgorithm.Algo[] ALL_ALGOS_SEL = {AUTOPART};
      for (AbstractAlgorithm.Algo algo : ALL_ALGOS_SEL) {
          algos_sel.add(algo);
      }
      AlgorithmRunner algoRunner = new AlgorithmRunner(algos_sel, 10, queries, 
    		  new AbstractAlgorithm.HDDAlgorithmConfig(
    				  BenchmarkTables.tpchLineitem(new BenchmarkTables.BenchmarkConfig(null, 10, 
    						  TableType.Default()))));
      algoRunner.runTPC_H_LineItem(true);
      String output = AlgorithmResults.exportResults(algoRunner.results);

      System.out.println(output);
  	
//  	Map<Integer,String> actionMap = AbstractAlgorithm.getActionMap();
//  	List<String> actions = new ArrayList<>();
//  	actions.add("[2,3]");
//  	actions.add("[8,9]");
//  	actions.add("[4,7]");
//  	actions.add("[4,7,9]");
//  	actions.add("[11,12]");
//  	actions.add("[4,7,8,9]");
//  	
//  	List<Integer>   actionsInteger =   AbstractAlgorithm.getActions(actions);
//  	for(Integer action: actionsInteger ) {
//  		System.out.println(action);
//  		
//  	}
    	
        
        
        
        
    }
    
    public AbstractAlgorithm.AlgorithmConfig getConfiguration(){
    	return this.config;
    }
    
    /*End Debugging End*/
}