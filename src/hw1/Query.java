package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.util.HashSet; 
import java.util.Set;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.AllColumns; 
import net.sf.jsqlparser.statement.select.SelectExpressionItem; 


public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
		FromItem from = sb.getFromItem();
        Relation cur = getFromTable((Table) from);
        
        if (sb.getJoins() != null) {
            for (Join join : sb.getJoins()) {
                Table right = (Table) join.getRightItem();
                Relation joinTable = getFromTable(right);
                EqualsTo joinExpression = (EqualsTo) join.getOnExpression();
                Column leftEx = (Column) joinExpression.getLeftExpression();
                Column rightEx = (Column) joinExpression.getRightExpression();
                int leftId = cur.getDesc().nameToId(leftEx.getColumnName());
                int rightId = joinTable.getDesc().nameToId(rightEx.getColumnName());
                cur = cur.join(joinTable, leftId, rightId);
            }
        }

        if (sb.getWhere() != null) {
            WhereExpressionVisitor wev = new WhereExpressionVisitor();
            sb.getWhere().accept(wev);
            int field = cur.getDesc().nameToId(wev.getLeft());
            cur = cur.select(field, wev.getOp(), wev.getRight());
        }
        HashMap<String, String> aliasHashMap = new HashMap<>();
        if (sb.getSelectItems() != null) {
            List<AggregateExpressionVisitor> columns = new ArrayList<>();

            for (SelectItem item : sb.getSelectItems()) {
                AggregateExpressionVisitor visitor = new AggregateExpressionVisitor();
                columns.add(visitor);
             // Handle aliasing here
                if (item instanceof AllColumns) {
                    AllColumns ac = (AllColumns) item;
                    visitor.visit(ac);
                } if (item instanceof SelectExpressionItem) {
                    SelectExpressionItem sei = (SelectExpressionItem) item;
                    
                    // If alias exists, add it to the HashMap
                    if (sei.getAlias() != null && sei.getExpression() instanceof Column) {
                        Column c = (Column) sei.getExpression();
                        aliasHashMap.put(c.getColumnName(), sei.getAlias().getName());
                    }
                    
                    if (sei.getExpression() instanceof Function) {
                        Function f = (Function) sei.getExpression();
                        visitor.visit(f);
                    } else if (sei.getExpression() instanceof Column) {
                        Column c = (Column) sei.getExpression();
                        visitor.visit(c);
                    }
                }
            }
            boolean isAggregate = false;
            for (AggregateExpressionVisitor column : columns) {
                if (column.isAggregate()) {
                    isAggregate = true;
                    break;
                }
            }
            TupleDesc desc = cur.getDesc();
            Set<Integer> columnIds = new HashSet<>();
            for (AggregateExpressionVisitor column : columns) {
                if (column.getColumn().equals("*")) {
                    for (int i = 0; i < desc.numFields(); i++) {
                        columnIds.add(desc.nameToId(desc.getFieldName(i)));
                    }
                } else {
                    columnIds.add(desc.nameToId(column.getColumn()));
                }
            }
            cur = cur.project(new ArrayList<>(columnIds));

            if (isAggregate) {
                boolean groupBy = false;
                if (sb.getGroupByColumnReferences()!=null){
                    groupBy = true;
                }

                for (AggregateExpressionVisitor column : columns) {
                    if (column.isAggregate()) {
                        cur = cur.aggregate(column.getOp(), groupBy);
                    }
                }
            }

        }

     
        if (!aliasHashMap.isEmpty()) {
            ArrayList<Integer> originalIndices = new ArrayList<>();
            ArrayList<String> aliasNames = new ArrayList<>();

            // Map original column names to their indices in TupleDesc
            for (String originalName : aliasHashMap.keySet()) {
                originalIndices.add(cur.getDesc().nameToId(originalName));
                aliasNames.add(aliasHashMap.get(originalName));
            }

            cur = cur.rename(originalIndices, aliasNames);
        }
        return cur;
		
	}
	private Relation getFromTable(Table tb) {
        Catalog c = Database.getCatalog();
        return new Relation(c.getDbFile(c.getTableId(tb.getName())).getAllTuples(), c.getDbFile(c.getTableId(tb.getName())).getTupleDesc());
    }
}
