package core.algo.vertical;

/*
 * JOCL - Java bindings for OpenCL
 * 
 * Copyright 2009 Marco Hutter - http://www.jocl.org/
 */

import static org.jocl.CL.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jocl.*;

import core.algo.vertical.AbstractAlgorithm.Algo;
import core.algo.vertical.AbstractAlgorithm.AlgorithmConfig;
import core.utils.PartitioningUtils;
import gnu.trove.set.hash.TIntHashSet;

/**
 * A small JOCL sample.
 */
public class HillClimbCL extends AbstractPartitioningAlgorithm {
    /**
     * The source code of the OpenCL program to execute
     */
    private static String programSource =
//        "__kernel void "+
//        "sampleKernel(__global const float *a,"+
//        "             __global const float *b,"+
//        "             __global float *c)"+
//        "{"+
//        "    int gid = get_global_id(0);"+
//        "    c[gid] = a[gid] * b[gid];"+
//        "}";
 
//    " kernel void sampleKernel(__global const int* A, __global const int* B, __global int* C,  __global const int* m)"
//    + "    {"
//    + "        int id= (int)get_global_id(0);"
//    + "        if( id<m[0] )"
//    + "        {"
//    + "            C[id]=A[id];"
//    + "        }"
//    + "        else"
//    + "        {"
//    + "            C[id]=B[id-m[0]];"
//    + "        }"
//    + "    }";
    		
    " kernel void sampleKernel(__global const int* A, __global const int* B, __global short* C,  __global const int* m, __global const int* n)"
     + "    {"
     + "        int id= (int)get_global_id(0);"
     + "		for (int k=0; k<n; k++){ if (A[id]==B[k]) {C[(id*n)+k]=0;} else {C[(id*n)+k]=1;}"
     + "		}"
     + "    }";
    
    private static String programSource2 =
//          "__kernel void "+
//          "sampleKernel(__global const float *a,"+
//          "             __global const float *b,"+
//          "             __global float *c)"+
//          "{"+
//          "    int gid = get_global_id(0);"+
//          "    c[gid] = a[gid] * b[gid];"+
//          "}";
   
//      " kernel void sampleKernel(__global const int* A, __global const int* B, __global int* C,  __global const int* m)"
//      + "    {"
//      + "        int id= (int)get_global_id(0);"
//      + "        if( id<m[0] )"
//      + "        {"
//      + "            C[id]=A[id];"
//      + "        }"
//      + "        else"
//      + "        {"
//      + "            C[id]=B[id-m[0]];"
//      + "        }"
//      + "    }";
      		
      " kernel void sampleKernel(__global const short* C, __global const short* B, __global const int* m)"
       + "    {"
       + "      int id= (int)get_global_id(0);"
       + "		for (int k=0; kif (A[id]==B[k]) {C[(id*n)+k]=0;} else {C[(id*n)+k]=1;}"
       + "    }";
    
    
    /*
	 * We do not use a cost table (as in the original algorithm)
	 * because the table becomes too big for large number of 
	 * attributes (~16GB for 46 attributes). Instead, it is not very
	 * expensive to calculate the costs repeatedly.
	 */
	//	private Map<String, Double> costTable;
	
	public HillClimbCL(AlgorithmConfig config) {
		super(config);
		type = Algo.HILLCLIMBCL;

//		costTable = new HashMap<String, Double>();
	}
	
	@Override
	public void doPartition() {
//		int[][] allGroups = getSetOfGroups(usageMatrix);
//		
//		for (int[] group : allGroups) {			
//			costTable.put(Arrays.toString(group), cm.getPartitionsCost(group));
//		}
		
		int[][] cand = new int[w.attributeCount][1];
		for(int i = 0; i < w.attributeCount; i++) {
			cand[i][0] = i;
		}
		double candCost = getCandCost(cand);
		double minCost;
		List<int[][]> candList = new ArrayList<int[][]>();
		int[][] R;
		int[] s;
		
		do {
			R = cand;
			minCost = candCost;
			candList.clear();
			for (int i = 0; i < R.length; i++) {
				for (int j = i + 1; j < R.length; j++) {
					cand = new int[R.length-1][];
					s = doMerge(R[i], R[j]);//
					for(int k = 0; k < R.length; k++) {
						if(k == i) {
							cand[k] = s;
						} else if(k < j) {
							cand[k] = R[k];
						} else if(k > j) {
							cand[k-1] = R[k];							
						}
					}
					candList.add(cand);
				}
			}
			if(!candList.isEmpty()) {
				cand = getLowerCostCand(candList);
				candCost = getCandCost(cand);
			}
		} while (candCost < minCost);

		partitioning = PartitioningUtils.getPartitioning(R);
	}
	
	private int[][] getLowerCostCand(List<int[][]> candList) {
		int indexOfLowest = 0;
		int index = 0;
		double lowestCost = Double.MAX_VALUE;
		for (int[][] cand : candList) {
			double cost = getCandCost(cand);
			if (lowestCost > cost) {
				indexOfLowest = index;
				lowestCost = cost;
			}
			index++;
		}
		return candList.get(indexOfLowest);
	}

	private int[] doMerge(int[] is, int[] is2) {
		//Here we concatenate them into a single array, in OpenCL
        int dstArray[] = new int[is.length+is2.length];
        
        Pointer srcA = Pointer.to(is);
        Pointer srcB = Pointer.to(is2);
        Pointer dst = Pointer.to(dstArray);
        int[] size_m = new int [1];
        size_m[0]= is.length+is2.length; 
        Pointer sizem = Pointer.to(size_m);


        // The platform, device type and device number
        // that will be used
        final int platformIndex = 0;
        final long deviceType = CL_DEVICE_TYPE_ALL;
        final int deviceIndex = 0;
        cl_context context;
        cl_program program;
        cl_command_queue commandQueue;
        
        // Enable exceptions and subsequently omit error checks in this sample
        CL.setExceptionsEnabled(true);

        // Obtain the number of platforms
        int numPlatformsArray[] = new int[1];
        clGetPlatformIDs(0, null, numPlatformsArray);
        int numPlatforms = numPlatformsArray[0];

        // Obtain a platform ID
        cl_platform_id platforms[] = new cl_platform_id[numPlatforms];
        clGetPlatformIDs(platforms.length, platforms, null);
        cl_platform_id platform = platforms[platformIndex];

        // Initialize the context properties
        cl_context_properties contextProperties = new cl_context_properties();
        contextProperties.addProperty(CL_CONTEXT_PLATFORM, platform);
        
        // Obtain the number of devices for the platform
        int numDevicesArray[] = new int[1];
        clGetDeviceIDs(platform, deviceType, 0, null, numDevicesArray);
        int numDevices = numDevicesArray[0];
        
        // Obtain a device ID 
        cl_device_id devices[] = new cl_device_id[numDevices];
        clGetDeviceIDs(platform, deviceType, numDevices, devices, null);
        cl_device_id device = devices[deviceIndex];

        // Create a context for the selected device
        context = clCreateContext(
            contextProperties, 1, new cl_device_id[]{device}, 
            null, null, null);
        
        // Create a command-queue for the selected device
        commandQueue = 
            clCreateCommandQueue(context, device, 0, null);

        // Allocate the memory objects for the input- and output data
        cl_mem memObjects[] = new cl_mem[4];
        memObjects[0] = clCreateBuffer(context, 
            CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
            Sizeof.cl_int * is.length, srcA, null); //Smallest
        memObjects[1] = clCreateBuffer(context, 
            CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR, //Biggest
            Sizeof.cl_int * is2.length, srcB, null);
        memObjects[2] = clCreateBuffer(context, 
            CL_MEM_READ_WRITE, 
            Sizeof.cl_short * is2.length * is.length, null, null);
        //Whatever memObjects[3] holds, it needs to be the size of a short
        memObjects[3] = clCreateBuffer(context, 
                CL_MEM_READ_WRITE, 
                Sizeof.cl_int, sizem, null);

        // Create the program from the source code
        program = clCreateProgramWithSource(context,
            1, new String[]{ programSource }, null, null);
        
        // Build the program
        clBuildProgram(program, 0, null, null, null, null);
        
        
        // Create the kernel
        cl_kernel kernel = clCreateKernel(program, "sampleKernel", null);
        
        // Set the arguments for the kernel
        clSetKernelArg(kernel, 0, 
            Sizeof.cl_mem, Pointer.to(memObjects[0]));
        clSetKernelArg(kernel, 1, 
            Sizeof.cl_mem, Pointer.to(memObjects[1]));
        clSetKernelArg(kernel, 2, 
            Sizeof.cl_mem, Pointer.to(memObjects[2]));
        clSetKernelArg(kernel, 3, 
                Sizeof.cl_mem, Pointer.to(memObjects[3]));
        
        // Set the work-item dimensions
        long global_work_size[] = new long[]{is2.length+is.length};
        long local_work_size[] = new long[]{1};
        
        // Execute the kernel
        clEnqueueNDRangeKernel(commandQueue, kernel, 1, null,
            global_work_size, local_work_size, 0, null, null);
        
        // Read the output data
        clEnqueueReadBuffer(commandQueue, memObjects[2], CL_TRUE, 0,
            is.length+is2.length * Sizeof.cl_int, dst, 0, null, null);
        
        // Release kernel, program, and memory objects
        clReleaseMemObject(memObjects[0]);
        clReleaseMemObject(memObjects[1]);
        clReleaseMemObject(memObjects[2]);
        clReleaseMemObject(memObjects[3]);
        clReleaseKernel(kernel);
        clReleaseProgram(program);
        clReleaseCommandQueue(commandQueue);
        clReleaseContext(context);
        
        
        
        
        
        
		//Here we pass the array to a set (which filters out duplicates)
		TIntHashSet set = new TIntHashSet();
		set.addAll(dstArray);
		
		//Here we return it as an array
		
//		set.addAll(is);
//	    set.addAll(is2);

		return set.toArray();
	}

	private double getCandCost(int[][] cand) {
		double sum = 0;

        sum = costCalculator.getPartitionsCost(cand);

        /*
		for (int[] item : cand) {
			sum += costCalculator.costForPartition(item);
//			System.out.println(Arrays.toString(item));
//			sum += costTable.get(Arrays.toString(item));
		} */

		return sum;
	}

//	private int[][] getSetOfGroups(int[][] usageMatrix) {
//		Map<Integer, List<Integer>> partitionAttributes = new HashMap<Integer,List<Integer>>();
//		List<Integer> attributes = new ArrayList<Integer>();
//		for(int i = 0; i < usageMatrix[0].length; i++)
//			attributes.add(i);
////		System.out.println("attrSize: "+attributes.size());
//		List<List<Integer>> psetattr = powerSetIter(attributes);
//		Collections.sort(psetattr, new ListComparator());
//		
//		int partitionCount = 0;
//		for (int p = psetattr.size()-1; p >= 0 ; p--) {
//			partitionAttributes.put(partitionCount++, psetattr.get(p));			
//		}
//				
//		int[][] primaryPartitions = new int[partitionAttributes.size()][];
//		int i = 0;
//		for(int p : partitionAttributes.keySet()){
//			List<Integer> attrs = partitionAttributes.get(p);
//			primaryPartitions[i] = new int[attrs.size()];
//			for(int j = 0; j < attrs.size(); j++)
//				primaryPartitions[i][j] = attrs.get(j);
//			i++;
//		}
//		
//		return primaryPartitions;
//	}
//
//	
//	public class ListComparator implements Comparator<List<Integer>> {
//	    @Override
//	    public int compare(List<Integer> o1, List<Integer> o2) {
//	        return o2.size()-o1.size();
//	    }
//	}
//
//	public static <T> List<List<T>> powerSetIter(Collection<T> list) {
//		List<List<T>> ps = new ArrayList<List<T>>();
//		ps.add(new ArrayList<T>()); // add the empty set
//
//		// for every item in the original list
//		for (T item : list) {
//			List<List<T>> newPs = new ArrayList<List<T>>();
//
//			for (List<T> subset : ps) {
//				// copy all of the current powerset's subsets
//				newPs.add(subset);
//
//				// plus the subsets appended with the current item
//				List<T> newSubset = new ArrayList<T>(subset);
//				newSubset.add(item);
//				newPs.add(newSubset);
//			}
//
//			// powerset is now powerset of list.subList(0, list.indexOf(item)+1)
//			ps = newPs;
//		}
//		ps.remove(new ArrayList<T>()); // remove the empty set
//		return ps;
//	}
}