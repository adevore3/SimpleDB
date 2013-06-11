package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc _td;
    private RecordId _rid;
    private Field[] _fields;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
    	if (td.numFields() < 1)
    		throw new IllegalArgumentException();
    	this._td = td;
    	this._fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this._td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this._rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this._rid = rid;
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
    	this._fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return this._fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
    	StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this._fields.length; i++) {
        	sb.append(this._fields[i].toString());
        	if (i != this._fields.length - 1)
        		sb.append('\t');
        	else
        		sb.append('\n');
        }
        return sb.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
    	ArrayList<Field> list = new ArrayList<Field>();
    	for (int i = 0; i < this._fields.length; i++)
    		list.add(this._fields[i]);
    	
        return list.iterator();
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this._td = td;
    }
    
    /**
     * Two Tuple objects are considered equal if they represent the same
     * fields.
     * 
     * @return True if this and o represent the same fields
     */
    @Override
    public boolean equals(Object o) {
        if (! (o instanceof Tuple))
        	return false;
        
        Tuple t = (Tuple) o;
        
        if (this._td.equals(t.getTupleDesc()))
        	for (int i = 0; i < this._fields.length; i++)
        		if (! this._fields[i].equals(t._fields[i]))
        			return false;
        
        return true;
    }
    
    /**
     * Two tuples with equal fields will have equal hash codes.
     * 
     * @return An int that is the same for equal Tuple objects.
     */
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
