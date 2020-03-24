package qp.optimizer;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

import qp.operators.Distinct;
import qp.operators.Join;
import qp.operators.JoinType;
import qp.operators.OpType;
import qp.operators.Operator;
import qp.operators.Project;
import qp.operators.Scan;
import qp.operators.Select;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;
import qp.utils.Schema;

public class GreedyOptimizer {
	SQLQuery sqlquery;

    ArrayList<Attribute> projectlist;
    ArrayList<String> fromlist;
    ArrayList<Condition> selectionlist;   // List of select conditons
    ArrayList<Condition> joinlist;        // List of join conditions
    ArrayList<Attribute> groupbylist;
    int numJoin;            // Number of joins in this query
    HashMap<String, Operator> tab_op_hash;  // Table name to the Operator
    Operator root;          // Root of the query plan tree

    public GreedyOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
        projectlist = sqlquery.getProjectList();
        fromlist = sqlquery.getFromList();
        selectionlist = sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        groupbylist = sqlquery.getGroupByList();
        numJoin = joinlist.size();
    }
    
    /**
     * prepare plan for the query using greedy heuristic
     **/
    public Operator getGreedyPlan() {

        if (sqlquery.getGroupByList().size() > 0) {
            System.err.println("GroupBy is not implemented.");
            System.exit(1);
        }

        if (sqlquery.getOrderByList().size() > 0) {
            System.err.println("Orderby is not implemented.");
            System.exit(1);
        }

        tab_op_hash = new HashMap<>();
        createScanOp();
        createSelectOp();
        if (numJoin != 0) {
            createJoinOp();
        }
        createProjectOp();
        if (this.sqlquery.isDistinct()) {
            createDistinctOp();
        }

        return root;
    }
    
    /**
     * Create Scan Operator for each of the table
     * * mentioned in from list
     **/
    public void createScanOp() {
        int numtab = fromlist.size();
        Scan tempop = null;
        for (int i = 0; i < numtab; ++i) {  // For each table in from list
            String tabname = fromlist.get(i);
            Scan op1 = new Scan(tabname, OpType.SCAN);
            tempop = op1;

            /** Read the schema of the table from tablename.md file
             ** md stands for metadata
             **/
            String filename = tabname + ".md";
            try {
                ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
                Schema schm = (Schema) _if.readObject();
                op1.setSchema(schm);
                _if.close();
            } catch (Exception e) {
                System.err.println("RandomInitialPlan:Error reading Schema of the table " + filename);
                System.err.println(e);
                System.exit(1);
            }
            tab_op_hash.put(tabname, op1);
        }

        if (selectionlist.size() == 0) {
            root = tempop;
            return;
        }

    }

    /**
     * Create Selection Operators for each of the
     * * selection condition mentioned in Condition list
     **/
    public void createSelectOp() {
        Select op1 = null;
        for (int j = 0; j < selectionlist.size(); ++j) {
            Condition cn = selectionlist.get(j);
            if (cn.getOpType() == Condition.SELECT) {
                String tabname = cn.getLhs().getTabName();
                Operator tempop = (Operator) tab_op_hash.get(tabname);
                op1 = new Select(tempop, cn, OpType.SELECT);
                /** set the schema same as base relation **/
                op1.setSchema(tempop.getSchema());
                modifyHashtable(tempop, op1);
            }
        }

        /** The last selection is the root of the plan tre
         ** constructed thus far
         **/
        if (selectionlist.size() != 0)
            root = op1;
    }
    
    /**
     * create join operators based on greedy heuristic of finding the table and join method with smallest cost to join.
     **/
    public void createJoinOp() {
    	HashSet<String> tableNames = new HashSet<>();
    	// Get all the names of tables that need to be joined
    	for (Condition c : joinlist) {
    		tableNames.add(c.getLhs().getTabName());
    		tableNames.add(((Attribute)c.getRhs()).getTabName());
    	}
    	Iterator<String> it = tableNames.iterator();
    	PlanCost pc = new PlanCost();
    	String startName = it.next();
    	Operator start = (Operator) tab_op_hash.get(startName);
    	while (it.hasNext()) {
    		String tableName = it.next();
    		Operator table = (Operator) tab_op_hash.get(tableName);
    		if (pc.getCost(table) < pc.getCost(start)) {
    			startName = tableName;
    			start = table;
    		}
    	}
    	tableNames.remove(startName);
    	String nextName = startName;
    	PriorityQueue<Operator> queue = new PriorityQueue<>((x, y) -> (int)(pc.getCost(x) - pc.getCost(y)));
    	HashMap<Operator, String> nameMap = new HashMap<>();
    	HashMap<String, Condition> conditionMap = new HashMap<>();
    	Join jn = null;
    	while (!tableNames.isEmpty()) {   		
    		for (Condition c : joinlist) {
        		if (c.getLhs().getTabName().equals(nextName)) {
        			String rhs = ((Attribute)(c.getRhs())).getTabName();
        			if (tableNames.contains(rhs)) {
        				Operator toAdd = tab_op_hash.get(rhs);
        				queue.add(toAdd);
        				nameMap.put(toAdd, rhs);
        				conditionMap.put(rhs, c);
        			}       			
        		} else if (((Attribute)(c.getRhs())).getTabName().equals(nextName)) {
        			String lhs = c.getLhs().getTabName();
        			if (tableNames.contains(lhs)) {
        				Operator toAdd = tab_op_hash.get(lhs);
        				queue.add(toAdd);
        				nameMap.put(toAdd, lhs);
        				c.flip();
        				conditionMap.put(lhs, c); 
        			}        			
        		}        		
    		}
    		Operator next = queue.remove();
    		nextName = nameMap.get(next);
    		while (!tableNames.contains(nextName)) {
    			next = queue.remove();
        		nextName = nameMap.get(next);
        		if (queue.isEmpty()) {
        			System.err.println("Error in greedy optimizer.");
                    System.exit(1);
        		}
    		}
    		Condition cn = conditionMap.get(nextName);
    		tableNames.remove(nextName);
    		jn = new Join(start, next, cn, OpType.JOIN);
            Schema newsche = start.getSchema().joinWith(next.getSchema());
            jn.setSchema(newsche);
            
            // Select a join type with smallest cost
            int numJMeth = JoinType.numJoinTypes();
            long minCost = Long.MAX_VALUE;
            for (int i = 0; i < numJMeth; i++) {
            	Join copy = (Join)jn.clone();
            	copy.setJoinType(i);
            	long cost = pc.getCost(copy);
            	if (cost < minCost) {
            		minCost = cost;
            		jn = copy;
            	}
            }
            start = jn;
            if (tableNames.isEmpty()) {
            	break;
            }
    	} 
    	
    	if (numJoin != 0)
            root = jn;    	
    }
    
    public void createProjectOp() {
        Operator base = root;
        if (projectlist == null)
            projectlist = new ArrayList<Attribute>();
        if (!projectlist.isEmpty()) {
            root = new Project(base, projectlist, OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }
    }
    
    public void createDistinctOp() {
        Operator base = root;
        if (projectlist == null)
            projectlist = new ArrayList<Attribute>();
        if (!projectlist.isEmpty()) {
            root = new Distinct(base, projectlist, OpType.DISTINCT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }
    }

    private void modifyHashtable(Operator old, Operator newop) {
        for (HashMap.Entry<String, Operator> entry : tab_op_hash.entrySet()) {
            if (entry.getValue().equals(old)) {
                entry.setValue(newop);
            }
        }
    }
    
}
