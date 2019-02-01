
-------------------------------------------------------------------------------------------------------------
--------------------------------------------- PARTITION SCHEME ----------------------------------------------
drop table if exists Fact_Optional;

-------------------------------------------------------------------------------------------------------------
-------------------------------------------- TEMPORAL DIMENSION ---------------------------------------------

create table Fact_Optional (
	dim_ID int not null, 
	factDate smalldatetime not null,
	dim_ValidFrom smalldatetime not null,
	factMeasure smallmoney not null,
	foreign key (dim_ID) references Dimension_Immutable (dim_ID),
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
-- ~ 3 minutes loading time
-------------------------------------------------------------------------------------------------------------
-- truncate table Fact_Optional;
drop table if exists #lookup;
select
	f.dim_ID,
	f.Timepoint as factDate,
	f.dim_ValidFrom
into
	#lookup
from (
	select 
		twine.*,
		MAX(case when Timeline = 'D' then Timepoint end) over (
			partition by dim_ID order by Timepoint
		) as dim_ValidFrom
	from (
		select
			dim_ID,
			factDate as Timepoint,
			'F' as Timeline
		from 
			dbo.Fact
		union all
		select
			dim_ID,
			dim_ValidFrom as Timepoint,
			'D' as Timeline
		from
			dbo.Dimension_Mutable
	) twine		
) f
where
	f.Timeline = 'F';

-- select top 100 * from #lookup;
-- select count(*) from #lookup;
insert into Fact_Optional (dim_ID, factDate, dim_ValidFrom, factMeasure)
select
	d.dim_ID,
	f.factDate,
	d.dim_ValidFrom,
	f.factMeasure
from
	#lookup l
join
	Fact f 
on 
	f.dim_ID = l.dim_ID
and
	f.factDate = l.factDate
join
	Dimension_Mutable d
on
	d.dim_ID = l.dim_ID
and
	d.dim_ValidFrom = l.dim_ValidFrom;

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Fact_Optional REBUILD;


-------------------------------------------------------------------------------------------------------------
-------------------------------------------- PERFORM THE TESTING --------------------------------------------
declare @runs int = 4; -- including one run for statistics
declare @DB_ID int = DB_ID();

if OBJECT_ID('Timings') is null
begin
	create table Timings (
		model varchar(42) not null,
		run int not null,
		query char(3) not null,
		executionTime int not null
	);
end

declare @startingTime datetime2(7);
declare @endingTime datetime2(7);

set nocount on;
declare @updateStatistics bit = 1;

declare @model varchar(42) = 'Optional';
delete Timings where model = @model and query in ('TIY', 'YIT', 'TOY');

while(@runs > 0) 
begin
	----------------------- Today is Yesterday -----------------------
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	drop table if exists #result_tiy;
	set @startingTime = SYSDATETIME();
	select 
		pit.dimProperty,
		f.numberOfFacts,
		f.avgMeasure
	into 
		#result_tiy
	from (
		select
			dim_Id,
			count(*) as numberOfFacts,
			avg(factMeasure) as avgMeasure
		from 
			Fact_Optional
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		pitDimension('2018-01-01') pit
	on 
		pit.dim_ID = f.dim_ID;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_tiy;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'TIY' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;
	
	----------------------- Yesterday is Today -----------------------
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	drop table if exists #result_yit;
	set @startingTime = SYSDATETIME();
	select 
		pit.dimProperty,
		f.numberOfFacts,
		f.avgMeasure
	into 
		#result_yit
	from (
		select
			dim_Id,
			count(*) as numberOfFacts,
			avg(factMeasure) as avgMeasure
		from 
			Fact_Optional
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		pitDimension('2014-01-01') pit
	on 
		pit.dim_ID = f.dim_ID;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_yit;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'YIT' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;

	----------------------- Today or Yesterday -----------------------
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	drop table if exists #result_toy;
	set @startingTime = SYSDATETIME();
	select
		dm.dimProperty,
		count(*) as numberOfFacts,
		avg(f.factMeasure) as avgMeasure
	into #result_toy
	from Fact_Optional f
	join Dimension_Mutable dm
	  on dm.dim_ID = f.dim_ID
	 and dm.dim_ValidFrom = f.dim_ValidFrom
	group by dm.dimProperty;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_toy;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'TOY' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;

	if @updateStatistics = 1 
	begin
		update statistics Dimension_Immutable with FULLSCAN;
		update statistics Dimension_Mutable with FULLSCAN;
		update statistics Fact_Optional with FULLSCAN;
		set @updateStatistics = 0;
	end

	set @runs = @runs - 1;
end

-- select * from Timings;
select
	rm.model,
	rm.query,
	round(rm.Median, 0) as Median,
 	round(1.96*ro.Deviation/sqrt(10), 0) as MarginOfError,
	round(ro.Average, 0) as Average,
	ro.Minimum,
	ro.Maximum
from (
	select distinct
		model, 
		query, 
		PERCENTILE_CONT(0.5) within group 
		(order by executionTime) over (partition by model, query) as Median
	from Timings
) rm
join (
	select
		model,
		query,
		avg(executionTime) as Average,
		min(executionTime) as Minimum,
		max(executionTime) as Maximum,
		stdevp(executionTime) as Deviation
	from Timings
	group by model, query
) ro
on
	ro.model = rm.model
and
	ro.query = rm.query
order by 
	1, 2;


