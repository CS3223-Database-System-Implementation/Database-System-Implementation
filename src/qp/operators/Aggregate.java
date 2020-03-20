/**
 * Scans the base relational table
 **/

package qp.operators;

import java.util.ArrayList;
import qp.utils.*;

/**
 * Scan operator - read data from a file
 */
public class Aggregate extends Operator {
    private ArrayList<AggregateAttribute> aggregate;
    ArrayList<Attribute> attrset;
    int[] attrIndex;
    Operator base;
    int batchSize;
    int tupleSize;
    ArrayList<Tuple> outputTuples;

    /**
     * Constructor - Save attributes to be aggregated
     */
    //
    public Aggregate(Operator base, ArrayList<AggregateAttribute> aggregate, ArrayList<Attribute> attrset, int[] attrIndex, int type, int tupleSize ) {
        super(type);
        this.base=base;
        this.aggregate = aggregate;
        this.attrset=attrset;
        this.attrIndex=attrIndex;
        this.tupleSize=tupleSize;
        this.outputTuples=new ArrayList<Tuple>();

    }

    /**
     * Open file prepare a stream pointer to read input file
     */
    public boolean open() {
        batchSize = Batch.getPageSize() / tupleSize;
        computeAggregate();
        return  true;
    }

    /**
     * Next operator - get a tuple from the file
     **/
    public Batch next() {
        Batch outBatch = new Batch(batchSize);

        if(outputTuples.size()==0){
            return null;
        }
        while (!outBatch.isFull()) {
            ArrayList<Object> present = new ArrayList<>();
            if(outputTuples.size()>0){
                Tuple basetuple=outputTuples.remove(0);
                // Keep all original fields
                for (int index=0; index<basetuple.data().size(); index++){
                    present.add(basetuple.data().get(index));
                }
                // Add new fields (Aggregate) to the back
                for (int j = 0; j < attrset.size(); j++) {
                  if (attrset.get(j).getAggType() != Attribute.NONE) {
                      for (AggregateAttribute AA : aggregate) {
                          if (AA.getAttributeIndex() == attrIndex[j] && AA.getAggregateType() == attrset.get(j).getAggType()) {
                              present.add(AA.getAggregatedVal());
                          }
                      }
                  }
                }
            } else {
                break;
            }
            Tuple outtuple = new Tuple(present);
            outBatch.add(outtuple);
        }
        return outBatch;
    }

    // Loop through all the tuples and compute their aggregate values
    public void computeAggregate(){
        while(true) {
            Batch inBatch = base.next();
            if(inBatch == null) {
                break;
            }
            while(inBatch.size()!=0) {
                Tuple t = inBatch.removeFirst();
                outputTuples.add(t);
                for (int i = 0; i < aggregate.size(); i++) {
                    aggregate.get(i).setAggregateVal(t);
                }
            }
        }

    }

}
