package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class JoinTest extends SimpleDbTestBase {

  int width1 = 2;
  int width2 = 3;
  DbIterator scan1;
  DbIterator scan2;
  DbIterator eqJoin;
  DbIterator gtJoin;
  DbIterator ltJoin;
  DbIterator gteqJoin;
  DbIterator lteqJoin;
  DbIterator neqJoin;

  /**
   * Initialize each unit test
   */
  @Before public void createTupleLists() throws Exception {
    this.scan1 = TestUtil.createTupleList(width1,
        new int[] { 1, 2,
                    3, 4,
                    5, 6,
                    7, 8 });
    this.scan2 = TestUtil.createTupleList(width2,
        new int[] { 1, 2, 3,
                    2, 3, 4,
                    3, 4, 5,
                    4, 5, 6,
                    5, 6, 7 });
    this.eqJoin = TestUtil.createTupleList(width1 + width2,
        new int[] { 1, 2, 1, 2, 3,
                    3, 4, 3, 4, 5,
                    5, 6, 5, 6, 7 });
    this.gtJoin = TestUtil.createTupleList(width1 + width2,
        new int[] {
                    3, 4, 1, 2, 3, // 1, 2 < 3
                    3, 4, 2, 3, 4,
                    5, 6, 1, 2, 3, // 1, 2, 3, 4 < 5
                    5, 6, 2, 3, 4,
                    5, 6, 3, 4, 5,
                    5, 6, 4, 5, 6,
                    7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 < 7
                    7, 8, 2, 3, 4,
                    7, 8, 3, 4, 5,
                    7, 8, 4, 5, 6,
                    7, 8, 5, 6, 7 });
    
    this.ltJoin = TestUtil.createTupleList(width1 + width2,
            new int[] {
            1, 2, 2, 3, 4, // 1 < 2, 3, 4, 5
            1, 2, 3, 4, 5,
            1, 2, 4, 5, 6,
            1, 2, 5, 6, 7,
            3, 4, 4, 5, 6, // 3 < 4, 5
            3, 4, 5, 6, 7 });
    
    this.gteqJoin = TestUtil.createTupleList(width1 + width2,
            new int[] {
    		3, 4, 1, 2, 3, // 1, 2, 3 <= 3
            3, 4, 2, 3, 4,
            3, 4, 3, 4, 5,
            5, 6, 1, 2, 3, // 1, 2, 3, 4, 5 <= 5
            5, 6, 2, 3, 4,
            5, 6, 3, 4, 5,
            5, 6, 4, 5, 6,
            5, 6, 5, 6, 7,
            7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 < 7
            7, 8, 2, 3, 4,
            7, 8, 3, 4, 5,
            7, 8, 4, 5, 6,
            7, 8, 5, 6, 7 });
    
    this.lteqJoin = TestUtil.createTupleList(width1 + width2,
            new int[] {
            1, 2, 1, 2, 3, // 1 <= 1, 2, 3, 4, 5
    		1, 2, 2, 3, 4,
            1, 2, 3, 4, 5,
            1, 2, 4, 5, 6,
            1, 2, 5, 6, 7,
            3, 4, 3, 4, 5, // 3 <= 3, 4, 5
            3, 4, 4, 5, 6,
            3, 4, 5, 6, 7,
            5, 6, 5, 6, 7 }); // 5 <= 5
    
    this.neqJoin = TestUtil.createTupleList(width1 + width2,
            new int[] {
    		3, 4, 1, 2, 3, // 1, 2, 4, 5 != 3
            3, 4, 2, 3, 4,
            3, 4, 4, 5, 6,
            3, 4, 5, 6, 7,
            5, 6, 1, 2, 3, // 1, 2, 3, 4 != 5
            5, 6, 2, 3, 4,
            5, 6, 3, 4, 5,
            5, 6, 4, 5, 6,
            7, 8, 1, 2, 3, // 1, 2, 3, 4, 5 != 7
            7, 8, 2, 3, 4,
            7, 8, 3, 4, 5,
            7, 8, 4, 5, 6,
            7, 8, 5, 6, 7 });
  }

  /**
   * Unit test for Join.getTupleDesc()
   */
  @Test public void getTupleDesc() {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    TupleDesc expected = Utility.getTupleDesc(width1 + width2);
    TupleDesc actual = op.getTupleDesc();
    assertEquals(expected, actual);
  }

  /**
   * Unit test for Join.rewind()
   */
  @Test public void rewind() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    while (op.hasNext()) {
      assertNotNull(op.next());
    }
    assertTrue(TestUtil.checkExhausted(op));
    op.rewind();

    eqJoin.open();
    Tuple expected = eqJoin.next();
    Tuple actual = op.next();
    assertTrue(TestUtil.compareTuples(expected, actual));
  }

  /**
   * Unit test for Join.getNext() using a &gt; predicate
   */
  @Test public void gtJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.GREATER_THAN, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    gtJoin.open();
    TestUtil.matchAllTuples(gtJoin, op);
  }
  
  /**
   * Unit test for Join.getNext() using a &gt; predicate
   */
  @Test public void ltJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.LESS_THAN, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    ltJoin.open();
    TestUtil.matchAllTuples(ltJoin, op);
  }

  /**
   * Unit test for Join.getNext() using an = predicate
   */
  @Test public void eqJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    eqJoin.open();
    TestUtil.matchAllTuples(eqJoin, op);
  }

  /**
   * Unit test for Join.getNext() using a &gt; predicate
   */
  @Test public void gteqJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.GREATER_THAN_OR_EQ, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    gteqJoin.open();
    TestUtil.matchAllTuples(gteqJoin, op);
  }
  
  /**
   * Unit test for Join.getNext() using a &gt; predicate
   */
  @Test public void lteqJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.LESS_THAN_OR_EQ, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    lteqJoin.open();
    TestUtil.matchAllTuples(lteqJoin, op);
  }

  /**
   * Unit test for Join.getNext() using an = predicate
   */
  @Test public void neqJoin() throws Exception {
    JoinPredicate pred = new JoinPredicate(0, Predicate.Op.NOT_EQUALS, 0);
    Join op = new Join(pred, scan1, scan2);
    op.open();
    neqJoin.open();
    TestUtil.matchAllTuples(neqJoin, op);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(JoinTest.class);
  }
}

