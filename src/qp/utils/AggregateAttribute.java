package qp.utils;

public class AggregateAttribute {

    private int attributeIndex;
    private int aggregateType;
    private Object aggregateVal;
    private int sum;
    private int count;


    public AggregateAttribute(int attributeIndex, int aggregateType) {
        this.attributeIndex = attributeIndex;
        this.aggregateType = aggregateType;
        if(aggregateType==Attribute.MAX){
            aggregateVal=null;
        } else if (aggregateType==Attribute.MIN){
            aggregateVal=null;
        } else if (aggregateType==Attribute.SUM){
            aggregateVal=0;
        } else if (aggregateType==Attribute.COUNT) {
            aggregateVal=0;
        } else if (aggregateType==Attribute.AVG) {
            aggregateVal=0;
            sum=0;
            count=0;
        }
    }
    
    public void setAggregateVal(Tuple t){
        Object val = t.dataAt(attributeIndex);
        if(val instanceof Integer){
            if(aggregateType==Attribute.MAX && (aggregateVal==null || (int)aggregateVal<(int)val)){
                aggregateVal=val;
            } else if (aggregateType==Attribute.MIN && (aggregateVal==null || ((int)aggregateVal>(int)val))){
                aggregateVal=val;
            } else if (aggregateType==Attribute.SUM){
                aggregateVal=(int)aggregateVal+(int)val;
            } else if (aggregateType==Attribute.COUNT) {
                aggregateVal=(int)aggregateVal+1;
            } else if (aggregateType==Attribute.AVG) {
                sum=sum+(int)val;
                count+=1;
                aggregateVal=sum/count;
            }
        } else if (val instanceof String){
            String valString = (String) val;
            if(aggregateType==Attribute.MAX && (aggregateVal==null || (valString.compareTo((String)aggregateVal)<0))){
                aggregateVal=valString;
            } else if (aggregateType==Attribute.MIN && (aggregateVal==null || valString.compareTo((String)aggregateVal)>0)){
                aggregateVal=valString;
            } else if (aggregateType==Attribute.COUNT) {
                aggregateVal=(int)aggregateVal+1;
            }
        }


    }
    public Object getAggregatedVal() {
        return aggregateVal;
    }

    public int getAttributeIndex(){
        return attributeIndex;
    }

    public int getAggregateType(){
        return aggregateType;
    }
}


