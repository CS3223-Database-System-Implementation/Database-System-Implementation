/**
 * Sort Merge join algorithm
 **/


package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SortMerge extends Join {
	private ExternalSortMerge leftSort;	
	private ExternalSortMerge rightSort;
	
    private int batchNum; 
    
    private ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
       

    public SortMerge(Join join) {
    	super(join.getLeft(), join.getRight(), join.getConditionList(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the left and right hand side into a file
     * * Check if the join tuple size is bigger than the tuple size given by user
     * * Opens the connections
     * * Sort both left and right file by performing External SortMerge
     **/
    @Override
    public boolean open() {
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
                
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        // Find the batch size
        int tupleSize = getSchema().getTupleSize();
        batchNum = Batch.getPageSize() / tupleSize;
        
        if (batchNum < 1) {
            //BUG IDENTIFIED  Throw error if PageSize is be smaller than TupleSize. Instead of going to an infinite loop.
            System.err.println(" PageSize must be bigger than join TupleSize. ");
            return false;
        }

        // Sort the 2 relations
        leftSort = new ExternalSortMerge(left, leftindex, numBuff);
        rightSort = new ExternalSortMerge(right, rightindex, numBuff);
        
        if (!(leftSort.open() && rightSort.open())) {
            return false;
        } else {
        	return true;
        }
    }

    /**
     * * And returns a page of output tuples
     **/
    @Override
    public Batch next() {
    	Batch joinBatch = findMatch();
    	if (!joinBatch.isEmpty()) {
    		return joinBatch;
    	} else {
    		return null;
    	}
    }
    
    /**
     * Close the operator
     */
    @Override
    public boolean close() {
    	return leftSort.close() && rightSort.close();
    }
    
    /**
     * from input buffers selects the tuples satisfying join condition
     **/
    private Batch findMatch() {
    	Batch joinBatch = new Batch(batchNum);
		Tuple leftTuple = leftSort.nextTuple();		//left pointer
		Tuple rightTuple = rightSort.nextTuple();	//right pointer
		
		Set<Tuple> leftSet = new HashSet<>();
		Set<Tuple> rightSet = new HashSet<>();
		
		while (!joinBatch.isFull() && leftTuple != null && rightTuple != null) {		
			int compare = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
			if (compare == 0) {
				leftSet.add(leftTuple);
		   		rightSet.add(rightTuple);
		   		Tuple secondRightTuple = rightSort.peekTuple();	//second right pointer
		   		if (secondRightTuple == null) {
		   			leftTuple = leftSort.nextTuple();
		   		} else {
		   			int compareRight = Tuple.compareTuples(rightTuple, secondRightTuple, rightindex, rightindex);
		            if (compareRight == 0) {
		        	   rightTuple = rightSort.nextTuple();	//next right tuple will have the same conditions
		           } else {
		        	   leftTuple = leftSort.nextTuple();
		           }
		       }
		       
			} else {
				join(leftSet, rightSet, joinBatch);
				leftSet.clear();
				rightSet.clear();
				
				if (compare < 0) {           
		    	leftTuple = leftSort.nextTuple();	//left is smaller than right, therefore pointer point to next left tuple
				} else {
		    	rightTuple = rightSort.nextTuple();	//right is smaller than left, therefore pointer point to next right tuple
					}
				}
			}
		
			return joinBatch;
		}
    
    private void join(Set<Tuple> leftSet, Set<Tuple> rightSet, Batch joinBatch) {
    	//perform join for multiple Tuple with same conditions
    	for (Tuple leftTuple : leftSet) {
    		for (Tuple rightTuple : rightSet) {
	        	Tuple joinTuple = leftTuple.joinWith(rightTuple);
	            joinBatch.add(joinTuple);
    		}
    	}
    }
    /**
     * returns a block of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     **/
    public Batch nextBlock(int size) {
    	int temp = batchNum;
    	batchNum = size;
    	Batch out = next();
    	batchNum = temp;
    	return out;
    }
    
}
