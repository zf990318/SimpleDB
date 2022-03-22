package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	private TDItem[] tdList;
	private int numField;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new tdIterator();
    }

    public class tdIterator implements Iterator<TDItem>{
    	
    	int i = 0;
		@Override
		public boolean hasNext() {
			// TODO Auto-generated method stub
			return i < tdList.length;
		}

		@Override
		public TDItem next() {
			// TODO Auto-generated method stub
			if(!hasNext()) {
				throw new NoSuchElementException();
			}
			return tdList[i++];
			
		}
    	
    }
    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     * @throws Exception 
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	if(typeAr.length == 0) {
    		throw new IllegalArgumentException("The typeAr must contain at least one element");
    	}
    	if(typeAr.length != fieldAr.length) {
    		throw new IllegalArgumentException("The length of typeAr must equal to the length of fieldAr");
    	}
    	
    	numField = typeAr.length;
    	tdList = new TDItem[numField];
    	for(int i = 0; i < numField; i++) {
    		tdList[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @throws Exception 
     */
    public TupleDesc(Type[] typeAr)  {
        // some code goes here
    	this(typeAr, new String[typeAr.length]);
    }
    
    

    public TupleDesc(TDItem[] mergeList)  {
		// TODO Auto-generated constructor stub
    	if(mergeList.length == 0) {
    		throw new IllegalArgumentException("The length of mergeList can not be 0");
    	}
    	numField= mergeList.length;
    	tdList = mergeList;
	}

	/**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numField ;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if(i >= tdList.length || i <0) {
    		throw new NoSuchElementException();
    	}
        return tdList[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if(i>= tdList.length || i<0) {
    		throw new NoSuchElementException();
    	}
        return tdList[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	if(name == null) {
    		throw new NoSuchElementException();
    	}
    	for(int i=0; i<tdList.length;i++) {
    		String fieldname = tdList[i].fieldName;
    		if(fieldname != null && fieldname.equals(name)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int size = 0;
    	for(int i = 0; i< tdList.length; i++) {
    		size += tdList[i].fieldType.getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     * @throws Exception 
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2)  {
        // some code goes here
    	TDItem[] mergeList = new TDItem[td1.tdList.length + td2.tdList.length];
		System.arraycopy(td1.tdList, 0, mergeList, 0, td1.tdList.length);
		System.arraycopy(td2.tdList, 0, mergeList, td1.tdList.length, td2.tdList.length);

        return new TupleDesc(mergeList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	if(o instanceof TupleDesc) {
			TupleDesc testee = (TupleDesc)o;
    		if(testee.numFields() == this.numFields()) {
    			for(int i = 0; i < numField; i++) {
    				if(!this.getFieldType(i).equals(testee.getFieldType(i))) {
    					return false;
    				}
    			}
    			return true;
    		}
    	}
    	return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String string = new String();
        string += ("Fields: ");
        for (int i =0; i < numField; i++) {
            string += (tdList[i].toString() + ", ");
        }
        string +=(numField + " Fields in all");
        return string;
    }
}