/**
 * Scans the base relational table
 **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

import qp.optimizer.BufferManager;

/**
 * Scan operator - read data from a file
 */
public class ExternalSort extends Operator {
	
    boolean eos;           // To indicate whether end of stream reached or not
    private ObjectInputStream sortedStream; // The input stream from which we read the sorted result.
    
    private ArrayList<Tuple> finalSortedTuples = new ArrayList<Tuple>();
    
    private Operator base;
    private int numBuffer;
    private Schema schema;
    private int batchSize; // Number of tuples per outbatch;
    private ArrayList<Attribute> projectList;
    private int[] projectListIndices;
    
    public ExternalSort(Operator base, ArrayList<Attribute> projectList, int[] projectListIndices, int type, int batchSize) {
        super(type);
    	this.base = base;
    	this.schema = base.getSchema();
    	
    	this.projectList=projectList;
    	this.projectListIndices=projectListIndices;
    	
    	this.numBuffer = BufferManager.getTotalBuffers();
    	this.batchSize = batchSize;
    	this.eos = false;
    
    }
    
	/**
     * Open file prepare a stream pointer to read input file
     */
    public boolean open() {
    	// Phase 1: Created sorted runs
    	int numOfSortedRuns = generateSortedRuns();
    	// Phase 2: Merge sorted runs
    	return mergeRuns(numOfSortedRuns, 1);
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        /** The file reached its end and no more to read **/
        if (eos) {
            return null;
        }
        Batch tuples = new Batch(batchSize);
        // Batch tuples = new Batch(2); //TESTING
        while (!tuples.isFull()) {
        	if(finalSortedTuples.size()>0) {
        		tuples.add(finalSortedTuples.remove(0));
        	} else {
        		eos=true;
        		break;
        	}
        }
        return tuples;
    }
    
    private int generateSortedRuns() {
    	// Read one page of tuples
    	// base will be the base that was stored inside Distinct Operator
    	// base.next will propagate to the root of the query tree -> likely to be Scan.next()
    	// Scan.next() will return a page of tuples
    	Batch inBatch = base.next();

    	//Based on the number of buffers, perform sorting to these number of tuples that  
    	//fill the entire buffer space
		int numRuns=0;
		boolean done=false;
		while(inBatch.size()!=0) {
			if(done==true) {
				break;
			}
			ArrayList<Tuple> tuplesInBuffer = new ArrayList<Tuple>();
			
			int totalTuplesBufferCanHold = numBuffer * batchSize;
			// int totalTuplesBufferCanHold = numBuffer * 2; //TESTING
			while(tuplesInBuffer.size() < totalTuplesBufferCanHold) {
				if(inBatch.size()==0) {
					inBatch=base.next();
					if(inBatch.size()==0) {
						done=true;
						break;
					}
				} 
				tuplesInBuffer.add(inBatch.removeFirst());
			}
			// Here, we have collected all the tuples and stored them to 
			// occupy all buffers in buffer space
			// To SORT
			tuplesInBuffer.sort(this::compareTuples);
			// Write to file
			String fileName = "run "+numRuns+" phase 0";
	    	TupleWriter TW = new TupleWriter(fileName, tuplesInBuffer.size());
	    	TW.open();
	    	for (int i=0; i<tuplesInBuffer.size(); i++) {
	    		TW.next(tuplesInBuffer.get(i));
	    		if(numRuns==0) {
	    			finalSortedTuples.add(tuplesInBuffer.get(i));
	    		} else {
	    			finalSortedTuples = new ArrayList<Tuple>();
	    		}
	    	}
			numRuns++;
		}
		
		return numRuns;
    }
    
    /*
     * Performs the Second part of external sort process which is merging.
     * Carries out merging for all phases.
     * Final sorted tuple list is stored in ArrayList<Tuple> finalSortedTuples
     */
    private boolean mergeRuns(int numOfSortedRuns, int phase) {
    	if(numOfSortedRuns<=1) {
    		return true;
    	}
    	// Do KWayMerge on all the runs in this phase
    	// The number of runs merged at each round of a phase is the number of buffers
    	int newNumOfSortedRuns = KWayMerge(numOfSortedRuns, numBuffer-1, phase);
    	return mergeRuns(newNumOfSortedRuns, phase+1);
    }
    
    /*
     * First reads in the tuples from the files saved in disk 
     * Performs on phase of KWay Merges according to the number of buffers available
     * @return number of sortedRuns generated
     */
    private int KWayMerge(int numOfSortedRuns, int numInputBuffers, int phase) {
    	// Read all the files...
    	int newNumOfSortedRuns=0;
    	//Each time for all the sortedRuns, take some sortedRuns only
    	//The number of sortedRuns to take = numInputBuffers
    	int fileNum=0;
    	int RunsLeftToMerge = numOfSortedRuns;
    	int numOfRunsToMerge = Math.min(numOfSortedRuns, numInputBuffers);
    	while(RunsLeftToMerge>0) {
    		//Read sortedRuns into Buffer
    		ArrayList<ArrayList<Tuple>> tuplesArray = readSortedRuns(fileNum, numOfRunsToMerge, phase);
    		// So KWayMerge and read to stream
	    	PriorityQueue<TupleIndexPair> heap = new PriorityQueue<TupleIndexPair>((o1,o2)->compareTuples(o1.getTuple(), o2.getTuple()));
	    	ArrayList<Tuple> sortedTuplesInBuffer = new ArrayList<Tuple>();
	    	//Add all first items in all lists to heap
			for(int PQ=0; PQ<tuplesArray.size(); PQ++) {
				ArrayList<Tuple> curr = tuplesArray.get(PQ);
				TupleIndexPair currPair = new TupleIndexPair(curr.remove(0), PQ); 
				heap.add(currPair);
			}
			//Perform Merge and combine them using KWayMerge
	    	while(true) {
	    		if(heap.size()==0) {
	    			break;
	    		}
	    		//Write out tuple from heap to new file according to Batch
	    		TupleIndexPair poppedPair = heap.poll();
	    		sortedTuplesInBuffer.add(poppedPair.getTuple());
	    		finalSortedTuples.add(poppedPair.getTuple());
	    		ArrayList<Tuple> toAddList = tuplesArray.get(poppedPair.getIndex());
	    		if(toAddList.size()>0) {
	    			TupleIndexPair toAddTuple = new TupleIndexPair(toAddList.remove(0), poppedPair.getIndex());
	    			heap.add(toAddTuple);
	    		}	
	    	}
	    	//Save result in file so that next phase can read.
	    	writeSortedRunsFromBuffer(sortedTuplesInBuffer, newNumOfSortedRuns, phase);
	    	fileNum+=numOfRunsToMerge;
	    	RunsLeftToMerge -= numOfRunsToMerge;
	    	numOfRunsToMerge = Math.min(RunsLeftToMerge, numInputBuffers);
	    	newNumOfSortedRuns+=1;
    	}
    	//Return the number of sorted runs in total
    	
    	if(newNumOfSortedRuns!=1) {
    		finalSortedTuples = new ArrayList<Tuple>();
    	}
    	return newNumOfSortedRuns;
    }
    
    /**
     * Writes the sortedRuns into a file on disk.
     */
    private void writeSortedRunsFromBuffer(ArrayList<Tuple> sortedTuplesInBuffer, int fileNum, int phase) {
    	TupleWriter TW = new TupleWriter("run "+fileNum+" phase "+(phase), sortedTuplesInBuffer.size());
    	TW.open();
    	for(Tuple t: sortedTuplesInBuffer) {
	    	TW.next(t);
    	}
    }
    
    /**
     * Reads the tuples saved in the files on disk.
     * Each file is saved in a single ArrayList<Tuple>
     * All Files are saved in an ArrayList of type ArrayList<Tuple>
     * @return ArrayList<ArrayList<Tuple>> which are all the lists of files to perform kWay merging on
     */
    private ArrayList<ArrayList<Tuple>> readSortedRuns(int fileNum, int numOfRunsToMerge, int phase) {
    	ArrayList<ArrayList<Tuple>> tuplesArray = new ArrayList<ArrayList<Tuple>>();
		for (int i=fileNum; i<fileNum+numOfRunsToMerge; i++) {
			TupleReader TR = new TupleReader("run "+i+" phase "+(phase-1), batchSize);
			TR.open();
			ArrayList<Tuple> thisRun = new ArrayList<>();
			Tuple currentTuple = TR.next();
			while(currentTuple!=null) {
				thisRun.add(currentTuple);
				currentTuple = TR.next();
			}
			tuplesArray.add(thisRun);
		}
		return tuplesArray;
    }
    
    /**
     * Compares two tuples based on the projected attribute.
     */
    private int compareTuples(Tuple tuple1, Tuple tuple2) {
        for (int projectKeyIndex: projectListIndices) {
            int result = Tuple.compareTuples(tuple1, tuple2, projectKeyIndex);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
    
    /**
     * Close the file.. This routine is called when the end of filed
     * * is already reached
     **/
    public boolean close() {
        try {
        	sortedStream.close();
        } catch (IOException e) {
            System.err.println("Scan: Error closing ");
            return false;
        }
        return true;
    }

    public Object clone() {
        String newtab = tabname;
        ExternalSort newscan = new ExternalSort ( base, projectList, projectListIndices, OpType.EXTERNALSORT, batchSize) ;
        newscan.setSchema((Schema) schema.clone());
        return newscan;
    }

}

final class TupleIndexPair {
	private Tuple t;
	private int listIndex;
	
	public TupleIndexPair(Tuple t, int listIndex) {
		this.t=t;
		this.listIndex=listIndex;
	}
	public int getIndex() {
		return listIndex;
	}
	public Tuple getTuple() {
		return t;
	}
}
