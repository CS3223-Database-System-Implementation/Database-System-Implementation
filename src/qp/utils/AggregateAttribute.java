package qp.utils;

public class AggregateAttribute {

    private int attributeIndex;
    private int aggregateType;
    private int aggregateVal;
    private int sum;
    private int count;


    public AggregateAttribute(int attributeIndex, int aggregateType) {
        this.attributeIndex = attributeIndex;
        this.aggregateType = aggregateType;
        if(aggregateType==Attribute.MAX){
            aggregateVal=Integer.MIN_VALUE;
        } else if (aggregateType==Attribute.MIN){
            aggregateVal=Integer.MAX_VALUE;
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
        int val = (int)t.dataAt(attributeIndex);
        if(aggregateType==Attribute.MAX && aggregateVal<val){
            aggregateVal=val;
        } else if (aggregateType==Attribute.MIN && aggregateVal>val){
            aggregateVal=val;
        } else if (aggregateType==Attribute.SUM){
            aggregateVal+=val;
        } else if (aggregateType==Attribute.COUNT) {
            aggregateVal+=1;
        } else if (aggregateType==Attribute.AVG) {
            sum+=val;
            count+=1;
            aggregateVal=sum/count;
        }

    }
    public int getAggregatedVal() {
        return aggregateVal;
    }

    public int getAttributeIndex(){
        return attributeIndex;
    }

    public int getAggregateType(){
        return aggregateType;
    }
}


