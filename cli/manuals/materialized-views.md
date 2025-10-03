---
display_name: "live materialized views"
---

# Live Materialized Views

Live materialized views in Turso are automatically updating database objects that maintain query results in real-time. Unlike traditional materialized views that require manual refresh, Turso's live materialized views use Incremental View Maintenance (IVM) to stay current with minimal overhead.

## Enabling Materialized Views

Materialized views are an experimental feature that must be explicitly enabled:

```bash
tursodb --experimental-views your_database.db
```

## What Makes Them Special

Traditional materialized views store a snapshot of query results that becomes stale as the underlying data changes. You must manually refresh them, which often means re-executing the entire query. That is a costly operation for large datasets.

Turso's materialized views are different. They automatically update themselves by tracking only the changes to the underlying tables. When you insert, update, or delete a row, the materialized view calculates just the incremental changes needed to stay current. This means:

- **No manual refresh required** - Views are always up-to-date
- **Efficient updates** - Only processes changed data, not the entire dataset
- **Real-time consistency** - Changes are reflected immediately
- **Scalable performance** - Update cost is proportional to the size of changes, not the size of the table

## How Incremental View Maintenance Works

Instead of re-computing the entire view when data changes, IVM tracks what has changed and updates only the affected portions of the materialized view. For example:

- When you insert a new row, IVM adds only that row's contribution to the view
- When you delete a row, IVM removes only that row's contribution
- When you update a row, IVM treats it as a delete of the old value followed by an insert of the new value

This approach is particularly powerful for aggregations. If you have a view that calculates the sum of millions of rows, adding one new row only requires adding that single value to the existing sum—not re-summing all million rows.

## Transactional Consistency

Because live materialized views are instantly updated, they are fully transactional. Views are updated inside the same transaction as the base table modifications, ensuring:

- **Atomic updates** - View changes are committed or rolled back together with base table changes
- **Consistency** - Views never show partial updates or inconsistent state
- **Isolation** - Other transactions see either the complete change or none of it
- **Durability** - View updates are persisted with the same guarantees as regular tables

If a transaction rolls back, all changes—including those to materialized views—are rolled back together.

## Creating Materialized Views

Create a materialized view using standard SQL syntax:

```sql
CREATE MATERIALIZED VIEW sales_summary AS
SELECT
    product_id,
    COUNT(*) as total_sales,
    SUM(amount) as revenue,
    AVG(amount) as avg_sale_amount
FROM sales
GROUP BY product_id;
```

Once created, you can query the materialized view like any table:

```sql
SELECT * FROM sales_summary WHERE revenue > 10000;
```

## Use Cases

Materialized views excel in scenarios where:

1. **Dashboard queries** - Complex aggregations that power real-time dashboards
2. **Reporting** - Pre-computed summaries for business intelligence
3. **Denormalization** - Maintaining denormalized data without manual updates
4. **Performance optimization** - Expensive joins or aggregations that are frequently queried

## Current Limitations

As an experimental feature, materialized views in Turso currently have some limitations:

- Not all SQL functions are supported in view definitions
- Views cannot reference other views

## Performance Considerations

While materialized views provide excellent query performance, they do add overhead to write operations. Each insert, update, or delete must also update any dependent materialized views. Consider this trade-off when designing your schema:

- Use materialized views for frequently-read, infrequently-written data
- Avoid creating too many materialized views on highly volatile tables
- Monitor the performance impact on write operations
