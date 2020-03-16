package qp.utils;

import java.util.ArrayList;
import java.util.Comparator;

public class SortedRunComparator implements Comparator<Tuple> {
    private ArrayList<Integer> listOfConditions;

    public SortedRunComparator(ArrayList<Integer> listOfConditions) {
        this.listOfConditions = listOfConditions;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        for (int i = 0; i < listOfConditions.size(); i++) {
            int compareResult = Tuple.compareTuples(t1, t2, listOfConditions.get(i));
            if (compareResult != 0) {
                return compareResult;
            }
        }
        return 0;
    }
}