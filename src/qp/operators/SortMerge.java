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
import java.util.LinkedList;
import java.util.List;

public class SortMerge extends Join {
	private ExternalSortMerge leftSort;	
	private ExternalSortMerge rightSort;
	
	private int leftJoinAttrIdx;	//Index of the join attributes in left table
	private int rightJoinAttrIdx;	//Index of the join attributes in right table
    private int batchNum; 
    
    private ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
       

    public SortMerge(Join join) {
    	super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
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


        leftJoinAttrIdx = getLeft().getSchema().indexOf(getCondition().getLhs());
        rightJoinAttrIdx = getRight().getSchema().indexOf((Attribute) getCondition().getRhs());

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
    	while (!joinBatch.isFull() && leftSort.peekTuple() != null && rightSort.peekTuple() != null) {
    		Tuple leftTuple = leftSort.peekTuple();
    		Tuple rightTuple = rightSort.peekTuple();
    		int comparison = Tuple.compareTuples(leftTuple, rightTuple, leftJoinAttrIdx, rightJoinAttrIdx);
            if (comparison < 0) {           
            	leftSort.nextTuple();
            } else if (comparison > 0) {
            	rightSort.nextTuple();
            } else {  
                Tuple joinTuple = leftTuple.joinWith(rightTuple);
                rightSort.nextTuple();
                joinBatch.add(joinTuple);
            }
    	}
    	return joinBatch;
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
