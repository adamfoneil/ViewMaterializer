# ViewMaterializer for SQL Server

I started this as an exercise to see if I could come up with a generic way of "materializing" views -- that is, keeping them in sync with a base table. This is for views that are too slow to run without criteria, but to which a business needs close to realtime access. There are lots of ETL and replication-based solutions for this very problem, but I wanted to approach it without scheduled jobs or other administration, and without triggers or other special application code changes. I wanted a C# "helper" object that could be dropped into any project.

My solution uses SQL Server [Change Tracking](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server?view=sql-server-2017) internally, and this does take some administration to turn on, but no ongoing management. My solution also has some requirements for objects you create that I'll go into below. I think you'll see it's still pretty easy to setup.

The heart of my solution is an abstract class [ViewMaterializer](https://github.com/adamosoftware/ViewMaterializer/blob/master/ViewMaterializer/ViewMaterializer.cs). Its constructor takes three arguments:
- the `sourceView` to sync from. This is intended to be a view that's too slow to query in production without criteria, but that is acceptable when filtered.

- the `targetTable` to sync to. Must be a base table.

- the `changeFunction` is a table function that accepts a change tracking `@version` argument. The columns of the function must correspond to the primary key columns
