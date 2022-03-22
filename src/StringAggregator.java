package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final Op what;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private static class aggregateFields {
        public String groupVal;
        public int count;

        public aggregateFields(String groupVal) {
            this.groupVal = groupVal;
            count = 0;
        }
    }
    private final HashMap<String, aggregateFields> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String groupVal = "";
        if (gbfield != NO_GROUPING) {
            groupVal = tup.getField(gbfield).toString();
        }

        aggregateFields aggregateField = groups.get(groupVal);
        if (aggregateField == null) {
            aggregateField = new aggregateFields(groupVal);
        }
        aggregateField.count++;

        groups.put(groupVal, aggregateField);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        LinkedList<Tuple> result = new LinkedList<>();
        int aggField = 1;
        TupleDesc td;

        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
            aggField = 0;
        }
        else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        for (String groupVal : groups.keySet()) {
            aggregateFields aggregateF = groups.get(groupVal);
            Tuple tup = new Tuple(td);

            if (gbfield != NO_GROUPING) {
                if (gbfieldtype == Type.INT_TYPE) {
                    tup.setField(0, new IntField(Integer.parseInt(groupVal)));
                }
                else {
                    tup.setField(0, new StringField(groupVal, Type.STRING_LEN));
                }
            }

            if (what == Op.COUNT) {
                tup.setField(aggField, new IntField(aggregateF.count));
            }

            result.add(tup);
        }

        DbIterator retVal;
        retVal = new TupleIterator(td, Collections.unmodifiableList(result));
        return retVal;
    }

}
