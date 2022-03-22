package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc tupleD;
    
    private RecordId rid;
    
    private Field[] field;
    
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	this.tupleD = td;
    	this.field = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleD;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	this.rid=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if (i < 0 || i >= this.field.length) {
            throw new IllegalArgumentException("Index is not vaild");
        }
        field[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	if(i < 0 || i >= this.field.length) {
    		throw new IllegalArgumentException("Index is not valid.");
    	}
        return field[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
    	String s = new String();
        for (int i = 0; i < field.length; i++) {
            if (i == field.length - 1) {
                s += field[i].toString();
                s += "\n";
            } else {
                s +=  field[i].toString();
                s += "\t";
            }
        }
        return s;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new fieldIterator();
    }
    
    public class fieldIterator implements Iterator<Field>{
    	
    	int i = 0;
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return i < field.length;
		}

		@Override
		public Field next() {
			// TODO Auto-generated method stub
			if(!hasNext()) {
				throw new NoSuchElementException();
			}
			return field[i++];
			
		}
    	
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.tupleD = td;
    }
}
