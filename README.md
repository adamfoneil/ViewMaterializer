# ViewMaterializer for SQL Server

I started this as an exercise to see if I could come up with a generic way of "materializing" views -- that is, keeping them in sync with a base table. This is for views that are too slow to run without criteria, but to which a business needs close to realtime access. There are lots of ETL and replication-based solutions for this very problem, but I wanted to approach it without scheduled jobs or other administration, and without triggers or other special application code changes. I wanted a C# "helper" object that could be dropped into any project.

My solution uses SQL Server [Change Tracking](https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server?view=sql-server-2017) internally, and this does take some administration to turn on, but no ongoing management. My solution also has some requirements for objects you create that I'll go into below. I think you'll see it's still pretty easy to setup.

The heart of my solution is an abstract class [ViewMaterializer](https://github.com/adamosoftware/ViewMaterializer/blob/master/ViewMaterializer/ViewMaterializer.cs). Its constructor takes three arguments:

- the `sourceView` to sync from. This is intended to be a view that's too slow to query in production without criteria, but that is acceptable when filtered.

- the `targetTable` to sync to. Must be a base table.

- the `changeFunction` is a table function that accepts a change tracking `@version` argument. The columns of the function must correspond to the primary key columns of `targetTable`. This is what the `ViewMaterializer` uses to tell what needs to be refreshed in the `targetTable`.

- the work is done by the [Execute](https://github.com/adamosoftware/ViewMaterializer/blob/master/ViewMaterializer/ViewMaterializer.cs#L36) method. You could set this up to run by a scheduled job, and in fact you'd need to in order to align it with your change tracking retention period. (So for example, if you set the retention period to 2 days, then you would need to call the `Execute` method at least every 2 days to ensure you didn't lose changes.)

Change tracking requires managing a version number in your application. `ViewMaterializer` is abstract because I didn't have a one-size-fits-all approach to persisting that number. Therefore, to implement `ViewMaterializer` you would need to implement methods for saving and retrieving the change tracking version. I show an example of this in the sample app, described next.

## Sample app

A sample conole app is [here](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/Program.cs). It is a highly simplified sales order database of items, regions, and orders. These are some things to note about the sample app:

- A SQL Script is [here](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/Sql/Script.sql) that creates the necessary objects for the sample database.

- I created sample data using my [Test Data Generator](https://github.com/adamosoftware/TestDataGen) project and Nuget package with usage [here](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/Program.cs#L30) and [here](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/Program.cs#L46).

- The `sourceView` is [here](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/Sql/Script.sql#L46). I used a pivot for my sample to demonstrate something half-way realistic in a business reporting scenario.

- The `targetTable` is created [here](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/Sql/Script.sql#L75) with a separate `rpt` schema. I executing the `SELECT...INTO` the target table after generating about 600K sample Sales records, then I added the proper constraints and primary key so my example would work.

- The `changeFunction` sample [here](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/Sql/Script.sql#L85).

- As I said above, `ViewMaterializer` is abstract because I didn't want to lock you into a single method of persisting the change tracking version number. You can see how I did it in the sample app with the [SampleViewMaterializer](https://github.com/adamosoftware/ViewMaterializer/blob/master/SampleApp/SampleViewMaterializer.cs) object, which uses my [DbDictionary.SqlServer](https://github.com/adamosoftware/SessionData) thing.

## Next steps

Although I got my sample to work, I don't have any decent benchmarks for looking at performance more broadly. Also, I don't have unit or integration tests, so I would probably want to focus on testing and performance analysis next.
