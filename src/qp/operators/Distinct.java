/**
 * To project out the required attributes from the result
 **/

package qp.operators;

import qp.utils.*;

import java.util.ArrayList;

public class Distinct extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    private ArrayList<Integer> indexArray;
    int batchsize;                 // Number of tuples per outbatch
    int numBuff;

    /**
     * The following fields are required during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;
    Tuple previousTuple;
    private ExternalSortMerge sortedBase;


    public Distinct(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.previousTuple=null;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setNumBuff(int numBuff ) {
        this.numBuff = numBuff;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {

        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        indexArray = new ArrayList<>();

        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
            indexArray.add(index);
        }
        sortedBase = new ExternalSortMerge(base, indexArray, numBuff);
        return sortedBase.open();
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        inbatch = sortedBase.next();
        if(inbatch==null) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            //To detect duplicates in a sorted input, we only check current tuple
            //against the previous tuple
            if(previousTuple==null || checkDuplicate(previousTuple, outtuple)) {
                outbatch.add(outtuple);
                previousTuple=outtuple;
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    /**
     * @param previoustuple
     * @param currenttuple
     * @return false if both tuples are same.
     */
    private boolean checkDuplicate(Tuple previoustuple, Tuple currenttuple) {

        //Compare every attribute of the tuples to check for duplicate
        for (int index: attrIndex) {
            if(Tuple.compareTuples(previoustuple, currenttuple, index)!=0) {
                return true;
            }
        }
        return false;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Distinct newproj = new Distinct(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}