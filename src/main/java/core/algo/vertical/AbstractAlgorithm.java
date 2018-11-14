package core.algo.vertical;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import core.costmodels.CostModel;
import core.costmodels.HDDCostModel;
import core.utils.PartitioningUtils;
import core.utils.TimeUtils.Timer;
import db.schema.entity.Table;
import db.schema.entity.Workload;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

public abstract class AbstractAlgorithm {

	public static enum Algo {ROW, AUTOPART, HILLCLIMB, HYRISE, INTGBASED, NAVATHE, O2P, OPTIMAL, TROJAN, LIFT, DREAM, COLUMN, 
		HILLCLIMBCL //VERY IMPORTANT, NEVER USE THIS.
		}
	public Algo type;

	protected Workload workload;
	protected Workload.SimplifiedWorkload w;

	protected Table t;

	protected Timer timer;

    protected AlgorithmConfig config;
    
    public List<Integer> actionSequence;

	public AbstractAlgorithm(AlgorithmConfig config) {

		// Why would a partitioning algorithm need to know an existing
		// partitioning?
		//this.partitions = t.partitions;

        this.config = config;

        this.t = config.getTable();
		this.workload = t.workload;
		this.w = workload.getSimplifiedWorkload();

		timer = new Timer();
	}

	public void partition() {
		List<Integer> actionSequence = new ArrayList<>();
		timer.start();
        if (w.queryCount == 0) {
            int[] partitioning = new int[w.attributeCount];
            if (this instanceof AbstractPartitioningAlgorithm) {
                ((AbstractPartitioningAlgorithm)this).partitioning = partitioning;
            } else {
                ((AbstractPartitionsAlgorithm)this).partitions = PartitioningUtils.getPartitioningMap(partitioning);
                ((AbstractPartitionsAlgorithm)this).bestSolutions = new TIntObjectHashMap<TIntHashSet>();
            }
        } else {
		   this.actionSequence =  doPartition();
        }
		timer.stop();
		t.partitions = getPartitions();
		//t.actionSequence = actionSequence;
	}

	public abstract List<Integer> doPartition();
	
	/**
	 * The collection of elements for each partition.
	 * 
	 * Note that this is the most general representation of the partitions, and
	 * internally non-overlapping partitions can be represented as a
	 * partitioning.
	 */
	public abstract TIntObjectHashMap<TIntHashSet> getPartitions();

	/*
	 * Four measures to evaluate a vertical partitioning method
	 * 
	 * 1. Total time taken to compute the partitioning 2. Total redundant bytes
	 * read due to the partitioning 3. Total attribute joins performed in the
	 * entire table
	 */

	public double getTimeTaken() {
		return timer.getElapsedTime();
	}

    /**
     * The number of elements of the search space of the algorithm.
     */
	public abstract long getCandidateSetSize();

	public abstract long getNumberOfIterations();

	public abstract static class AlgorithmConfig implements Cloneable {
		protected Table table;
        public Workload.SimplifiedWorkload w;
        public CostModel.CMType type;
        public List<Integer> actionSequence;

		public AlgorithmConfig(Table table) {
			this.table = table;
            this.w = table.workload.getSimplifiedWorkload();
		}

		public Table getTable() {
			return table;
		}

        public void setTable(Table table) {
            this.table = table;
            this.w = table.workload.getSimplifiedWorkload();
        }

        public void setW(Workload.SimplifiedWorkload w) {
            this.w = w;
        }
        
        @Override
        public abstract AlgorithmConfig clone();
    }

    public static class HDDAlgorithmConfig extends AlgorithmConfig {

        public HDDCostModel costModel;

        public HDDAlgorithmConfig(Table table, HDDCostModel costModel) {
            super(table);

            this.costModel = costModel;
            this.type = costModel.type;
        }

        public HDDAlgorithmConfig(Table table) {
            super(table);

            this.costModel = (HDDCostModel)
                    CostModel.getCostModel(CostModel.CMType.HDD, table.workload.getSimplifiedWorkload());
            this.type = CostModel.CMType.HDD;
        }

        @Override
        public void setTable(Table table) {
            super.setTable(table);
            costModel.setW(table.workload.getSimplifiedWorkload());
        }

        @Override
        public void setW(Workload.SimplifiedWorkload w) {
            costModel = (HDDCostModel)CostModel.getCostModel(CostModel.CMType.HDD, w);
        }
        
        public void setWSel(Workload.SimplifiedWorkload w) {
            costModel = (HDDCostModel)CostModel.getCostModel(CostModel.CMType.HDDSelectivity, w);
        }

        @Override
        public AlgorithmConfig clone() {
            return new HDDAlgorithmConfig(table, costModel);
        }
    }

    public static class MMAlgorithmConfig extends AlgorithmConfig {

        public MMAlgorithmConfig(Table table) {
            super(table);

            this.type = CostModel.CMType.MM;
        }

        @Override
        public AlgorithmConfig clone() {
            return new MMAlgorithmConfig(table);
        }
    }
	
	public static AbstractAlgorithm getAlgo(Algo algo, AlgorithmConfig config){
		switch(algo){
		case AUTOPART:	return new AutoPart(config);
		case HILLCLIMB:	return new HillClimb(config);
		case HYRISE:	return new HYRISE(config);
		case NAVATHE:	return new NavatheAlgorithm(config);
		case O2P:		return new O2P(config);
		case OPTIMAL:	return new Optimal(config);
		case TROJAN:	return new TrojanLayout(config);
        case DREAM:     return new DreamPartitioner(config);
        case HILLCLIMBCL:  return new HillClimbCL(config);
		default:		return null;
		}
	}
	
	
	public static Map<Integer,String> getActionMap() {
		Map<Integer, String> actionMap = new HashMap<>();
		actionMap.put(0,"[0,1]");
		actionMap.put(1,"[0,2]");
		actionMap.put(2,"[0,3]");
		actionMap.put(3,"[0,4]");
		actionMap.put(4,"[0,5]");
		actionMap.put(5,"[0,6]");
		actionMap.put(6,"[0,7]");
		actionMap.put(7,"[0,8]");
		actionMap.put(8,"[0,9]");
		actionMap.put(9,"[0,10]");
		actionMap.put(10,"[0,11]");
		actionMap.put(11,"[0,12]");
		actionMap.put(12,"[0,13]");
		actionMap.put(13,"[0,14]");
		actionMap.put(14,"[0,15]");

		actionMap.put(15,"[1,2]");
		actionMap.put(16,"[1,3]");
		actionMap.put(17,"[1,4]");
		actionMap.put(18,"[1,5]");
		actionMap.put(19,"[1,6]");
		actionMap.put(20,"[1,7]");
		actionMap.put(21,"[1,8]");
		actionMap.put(22,"[1,9]");
		actionMap.put(23,"[1,10]");
		actionMap.put(24,"[1,11]");
		actionMap.put(25,"[1,12]");
		actionMap.put(26,"[1,13]");
		actionMap.put(27,"[1,14]");
		actionMap.put(28,"[1,15]");

		actionMap.put(29,"[2,3]");
		actionMap.put(30,"[2,4]");
		actionMap.put(31,"[2,5]");
		actionMap.put(32,"[2,6]");
		actionMap.put(33,"[2,7]");
		actionMap.put(34,"[2,8]");
		actionMap.put(35,"[2,9]");
		actionMap.put(36,"[2,10]");
		actionMap.put(37,"[2,11]");
		actionMap.put(38,"[2,12]");
		actionMap.put(39,"[2,13]");
		actionMap.put(40,"[2,14]");
		actionMap.put(41,"[2,15]");

		actionMap.put(42,"[3,4]");
		actionMap.put(43,"[3,5]");
		actionMap.put(44,"[3,6]");
		actionMap.put(45,"[3,7]");
		actionMap.put(46,"[3,8]");
		actionMap.put(47,"[3,9]");
		actionMap.put(48,"[3,10]");
		actionMap.put(49,"[3,11]");
		actionMap.put(50,"[3,12]");
		actionMap.put(51,"[3,13]");
		actionMap.put(52,"[3,14]");
		actionMap.put(53,"[3,15]");

		actionMap.put(54,"[4,5]");
		actionMap.put(55,"[4,6]");
		actionMap.put(56,"[4,7]");
		actionMap.put(57,"[4,8]");
		actionMap.put(58,"[4,9]");
		actionMap.put(59,"[4,10]");
		actionMap.put(60,"[4,11]");
		actionMap.put(61,"[4,12]");
		actionMap.put(62,"[4,13]");
		actionMap.put(63,"[4,14]");
		actionMap.put(64,"[4,15]");

		actionMap.put(65,"[5,6]");
		actionMap.put(66,"[5,7]");
		actionMap.put(67,"[5,8]");
		actionMap.put(68,"[5,9]");
		actionMap.put(69,"[5,10]");
		actionMap.put(70,"[5,11]");
		actionMap.put(71,"[5,12]");
		actionMap.put(72,"[5,13]");
		actionMap.put(73,"[5,14]");
		actionMap.put(74,"[5,15]");

		actionMap.put(75,"[6,7]");
		actionMap.put(76,"[6,8]");
		actionMap.put(77,"[6,9]");
		actionMap.put(78,"[6,10]");
		actionMap.put(79,"[6,11]");
		actionMap.put(80,"[6,12]");
		actionMap.put(81,"[6,13]");
		actionMap.put(82,"[6,14]");
		actionMap.put(83,"[6,15]");

		actionMap.put(84,"[7,8]");
		actionMap.put(85,"[7,9]");
		actionMap.put(86,"[7,10]");
		actionMap.put(87,"[7,11]");
		actionMap.put(88,"[7,12]");
		actionMap.put(89,"[7,13]");
		actionMap.put(90,"[7,14]");
		actionMap.put(91,"[7,15]");

		actionMap.put(92,"[8,9]");
		actionMap.put(93,"[8,10]");
		actionMap.put(94,"[8,11]");
		actionMap.put(95,"[8,12]");
		actionMap.put(96,"[8,13]");
		actionMap.put(97,"[8,14]");
		actionMap.put(98,"[8,15]");

		actionMap.put(99,"[9,10]");
		actionMap.put(100,"[9,11]");
		actionMap.put(101,"[9,12]");
		actionMap.put(102,"[9,13]");
		actionMap.put(103,"[9,14]");
		actionMap.put(104,"[9,15]");

		actionMap.put(105,"[10,11]");
		actionMap.put(106,"[10,12]");
		actionMap.put(107,"[10,13]");
		actionMap.put(108,"[10,14]");
		actionMap.put(109,"[10,15]");

		actionMap.put(110,"[11,12]");
		actionMap.put(111,"[11,13]");
		actionMap.put(112,"[11,14]");
		actionMap.put(113,"[11,15]");

		actionMap.put(114,"[12,13]");
		actionMap.put(115,"[12,14]");
		actionMap.put(116,"[12,15]");

		actionMap.put(117,"[13,14]");
		actionMap.put(118,"[13,15]");

		actionMap.put(119,"[14,15]");
		
		return actionMap;
		
	}
	
	public static List<Integer> getActions(List<String> actionSequence){
		
		List<Integer> actionMapSequence = new ArrayList<>();
		Map<Integer, String> actionMap = getActionMap();
		
	    Deque<List<String>> stackOfPreviousAction = new ArrayDeque<>();
	    
	    for(String action: actionSequence) {
	    
	    	String actionRemoveSpace = action.replace(" ", "");
	    	String clearAction = actionRemoveSpace.replace("[", "").replace("]", "");
	    	List<String> actionElements = new ArrayList<>(Arrays.asList(clearAction.split(",")));
	    	
	    	if(actionElements.size() == 2) {
	    		 for (Entry<Integer, String> entry : actionMap.entrySet()) {
	    	            if (entry.getValue().equals(actionRemoveSpace)) {
	    	            	actionMapSequence.add(entry.getKey());
	    	            }
	    	        }
	    	}
	    	else if (actionElements.size() > 2) {
	    		List<String> actionElementsDifference = actionElements;
	    		Iterator iterator = stackOfPreviousAction.iterator(); 
	    		while (iterator.hasNext()) {
	    			List<String> value = (List<String>) iterator.next();
	    	        if (value.get(0).equals(actionElements.get(0))) {
	    	        	actionElementsDifference.removeAll(value);
	    	        	String valueForSearch = "[" + value.get(0) + "," + actionElementsDifference.get(0) + "]";
	    	       	 	for (Entry<Integer, String> entry : actionMap.entrySet()) {
		    	            if (entry.getValue().equals(valueForSearch)) {
		    	            	actionMapSequence.add(entry.getKey());
		    	            }
		    	        }
	    	       	 	break;
	    	     	    	        	
	    	        }
	    			
	    		}
	    		
	    	}
	    	stackOfPreviousAction.push(actionElements);
	  
	    }
		
		
		
		
		return actionMapSequence;
		
	}
	
	
}