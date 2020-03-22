/**
 * To project out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.AggregateAttribute;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch

    ArrayList<AggregateAttribute> aggregate = new ArrayList<AggregateAttribute>();  // Set of attributes to do aggregation on
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

    Operator Aggregate;
    boolean performAggregate; // Boolean variable to indicate whether the projected variables needs to be aggregated
    Tuple previousTuple;
    public Project(Operator base, ArrayList<Attribute> as, int type) {
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

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the distinct columns to be
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
        performAggregate = false;

        //Go through all attributes to be projected.
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;

            // Check if projected attribute needs to be aggregated
            if (attr.getAggType() != Attribute.NONE) {
                performAggregate=true;
                aggregate.add( new AggregateAttribute(index, attr.getAggType()));
            }
        }

        if(performAggregate==true){
            Aggregate = new Aggregate(base, aggregate, attrset, attrIndex, OpType.AGGREGATE, tuplesize );
            Aggregate.open(); //Need to read through all the values and return here the required values
        }

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize); // Remember to modify batchsize to 2 for testing

        if(performAggregate==true){
            inbatch = Aggregate.next();
        } else {
            inbatch = base.next();
        }

        if (inbatch == null) {
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            ArrayList<Object> present = new ArrayList<>();
            boolean hasAggregate = false;
            for (int j = 0; j < attrset.size(); j++) {
                if (attrset.get(j).getAggType() != Attribute.NONE) {
                    hasAggregate = true;
                    int count=0;
                    for (AggregateAttribute AA : aggregate) {
                        count+=1;
                        if (AA.getAttributeIndex() == attrIndex[j] && AA.getAggregateType() == attrset.get(j).getAggType()) {
                            Object data = basetuple.dataAt(base.getSchema().getNumCols()+count-1);
                            present.add(data);
                            break;
                        }
                    }
                } else {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    present.add(data);
                }
            }
            Tuple outtuple = new Tuple(present);

            // If Aggregated Attributes are present in tuple, check for duplicates and remove them
            if(hasAggregate && (previousTuple==null || checkDuplicate(previousTuple, outtuple))){
                outbatch.add(outtuple);
                previousTuple = outtuple;

            } else if(!hasAggregate) {
                outbatch.add(outtuple);
                previousTuple = outtuple;
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
        for(int i=0; i<currenttuple.data().size(); i++){
            if(Tuple.compareTuples(previoustuple, currenttuple, i)!=0) {
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
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
