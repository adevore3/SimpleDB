package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	private TDItem[] _TDItem;

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
    	ArrayList<TDItem> list = new ArrayList<TDItem>(this._TDItem.length);
    	for (int i = 0; i < this._TDItem.length; i++)
    		list.set(i, this._TDItem[i]);
    	
        return list.iterator();
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
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	if (typeAr.length > 0 && typeAr.length == fieldAr.length) {
	        this._TDItem = new TDItem[typeAr.length];
	        for (int i = 0; i < typeAr.length; i++)
	        	this._TDItem[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }
 
    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this._TDItem.length;
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
    	if (i < 0 || i > this._TDItem.length - 1)
    		throw new NoSuchElementException();
        return this._TDItem[i].fieldName;
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
    	if (i < 0 || i > this._TDItem.length - 1)
    		throw new NoSuchElementException();
        return this._TDItem[i].fieldType;
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
    	if (name != null)
	        for (int i = 0; i < this._TDItem.length; i++)
	        	if (this._TDItem[i].fieldName != null)
//	        		if (name.endsWith(this._TDItem[i].fieldName))
	        		if (this._TDItem[i].fieldName.equals(name))
	        			return i;
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (int i = 0; i < this._TDItem.length; i++)
    		size += this._TDItem[i].fieldType.getLen();
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
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	int numOfFields = td1._TDItem.length + td2._TDItem.length;
        Type[] typeAr = new Type[numOfFields];
        String[] fieldAr = new String[numOfFields];
        
        for (int i = 0; i < numOfFields; i++) {
        	if (i >= td1._TDItem.length)
        		typeAr[i] = td2._TDItem[i - td1._TDItem.length].fieldType;
        	else
        		typeAr[i] = td1._TDItem[i].fieldType;
        	
        	if (i >= td1._TDItem.length)
        		fieldAr[i] = td2._TDItem[i - td1._TDItem.length].fieldName;
        	else
        		fieldAr[i] = td1._TDItem[i].fieldName;
        }
        
        return new TupleDesc(typeAr, fieldAr);
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
    @Override
    public boolean equals(Object o) {
    	if (! (o instanceof TupleDesc))
    		return false;
    	
    	TupleDesc td = (TupleDesc) o;
    	
        if (this.getSize() == td.getSize()) {
        	for (int i = 0; i < this._TDItem.length; i++) {
        		if (! this._TDItem[i].fieldType.equals(td._TDItem[i].fieldType))
        			return false;
        	}
        	return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
    	return this.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this._TDItem.length; i++) {
        	sb.append(this._TDItem[i].toString());
        	if (i != this._TDItem.length - 1)
        		sb.append(",");
        }
        return sb.toString();
    }
}
