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
	
	//private ExternalSort leftExternalSort;
	//private ExternalSort rightExternalSort;

	private int leftJoinAttrIdx;
	private int rightJoinAttrIdx;
    private int batchNum;
    
    private ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    private ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    
    int[] leftAttrIndex;
    int[] rightAttrIndex;


    public SortMerge(Join join) {
    	super(join.getLeft(), join.getRight(), join.getCondition(), join.getOpType());
        schema = join.getSchema();
        jointype = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    @Override
    public boolean open() {
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        
        leftAttrIndex = new int[conditionList.size()];
        rightAttrIndex = new int[conditionList.size()];
        
        int i = 0;
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
            
            leftAttrIndex[i] = left.getSchema().indexOf(leftattr);
            rightAttrIndex[i] = right.getSchema().indexOf(rightattr);
            i++;
        }

        // Find the batch size
        int tupleSize = getSchema().getTupleSize();
        int temp = Batch.getPageSize();
        batchNum = Batch.getPageSize() / tupleSize;
        
        if (batchNum < 1) {
            //BUG IDENTIFIED  Throw error if PageSize is be smaller than TupleSize. Instead of going to an infinite loop.
            System.err.println(" PageSize must be bigger than join TupleSize. ");
            return false;
        }

        // Find the index of join attribute of in each relation
        //int leftSize = getLeft().getSchema().getTupleSize();
        //int rightSize = getRight().getSchema().getTupleSize();

        leftJoinAttrIdx = getLeft().getSchema().indexOf(getCondition().getLhs());
        rightJoinAttrIdx = getRight().getSchema().indexOf((Attribute) getCondition().getRhs());

        // Sort the 2 relations
        leftSort = new ExternalSortMerge(left, leftindex, numBuff);
        rightSort = new ExternalSortMerge(right, rightindex, numBuff);
        
        //leftExternalSort = new ExternalSort(left, null, leftAttrIndex, OpType.EXTERNALSORT, batchNum);
        //rightExternalSort = new ExternalSort(right, null, rightAttrIndex, OpType.EXTERNALSORT, batchNum);        

        if (!(leftSort.open() && rightSort.open())) {
            return false;
        } else {
        	return true;
        }
    }

    @Override
    public Batch next() {
    	Batch joinBatch = findMatch();
    	if (!joinBatch.isEmpty()) {
    		return joinBatch;
    	} else {
    		return null;
    	}
    }
    
    @Override
    public boolean close() {
    	//return leftExternalSort.close() && rightExternalSort.close();
    	return true;
    }
    
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
}