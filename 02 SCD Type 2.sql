drop table if exists Fact_SCD2;
drop table if exists Dimension_SCD2;

-------------------------------------------------------------------------------------------------------------
------------------------------------------------ SCD Type 2 -------------------------------------------------
create table Dimension_SCD2 (
	dim_ID int identity(1,1) not null, -- needs to be a larger data type now
	dim_ValidFrom smalldatetime not null,
	dimStatic int not null, -- we will put the dim_ID from Dimension_Immutable here
	dimProperty char(42) not null,
	primary key (dim_ID asc),
	unique (dimStatic asc, dim_ValidFrom desc) -- necessary for performance reasons
); -- note that partitioning is not possible since dim_ValidFrom is not part of the primary key

create table Fact_SCD2 (
	dim_ID int not null, 
	factDate smalldatetime not null,
	factMeasure smallmoney not null,
	foreign key (dim_ID) references Dimension_SCD2 (dim_ID),
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

-------------------------------------------------------------------------------------------------------------
----------------------------------------- POPULATING THE DIMENSION ------------------------------------------
insert into Dimension_SCD2 (dim_ValidFrom, dimStatic, dimProperty)
select
	dim_ValidFrom,
	dim_ID,
	dimProperty
from
	Dimension_Mutable;

-- select count(*), count(distinct dim_ID), count(distinct dimStatic) from Dimension_SCD2;

-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
-- ~ 3 minutes loading time
-------------------------------------------------------------------------------------------------------------
-- truncate table Fact_SCD2;
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
insert into Fact_SCD2 (dim_ID, factDate, factMeasure)
select
	d.dim_ID,
	f.factDate,
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
	Dimension_SCD2 d
on
	d.dimStatic = l.dim_ID
and
	d.dim_ValidFrom = l.dim_ValidFrom;

-- select top 100 * from Fact_SCD2;
-- select count(*), count(distinct dim_ID) from Fact_SCD2;
-- 52427789	10373753

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Dimension_SCD2 REBUILD;
ALTER INDEX ALL ON Fact_SCD2 REBUILD;

-------------------------------------------------------------------------------------------------------------
----------------------------- CREATING A POINT-IN-TIME PARAMETRIZED VIEW ------------------------------------
drop function if exists pitDimension_SCD2;
go
create function pitDimension_SCD2 (
	@timepoint smalldatetime
) 
returns table as return
select
	d.dim_ID,
	dm_in_effect.dim_ValidFrom,
	dm_in_effect.dimStatic,
	dm_in_effect.dimProperty
from 
	Dimension_SCD2 d
cross apply (
	select top 1 
		s.*
	from 
		Dimension_SCD2 s
	where
		s.dimStatic = d.dimStatic
	and
		s.dim_ValidFrom <= @timepoint
	order by
		s.dim_ValidFrom desc
) dm_in_effect;
go

-- select count(*), count(distinct dimStatic) from pitDimension_SCD2('20180101')

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

declare @model varchar(42) = 'Type 2';
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
		 count(*) as numberOfFacts,
		 avg(f.factMeasure) as avgMeasure
	into #result_tiy
	from Fact_SCD2 f
	join pitDimension_SCD2('2018-01-01') pit
	  on pit.dim_ID = f.dim_ID
	where f.factDate between '2014-01-01' and '2014-12-31'		   
	group by pit.dimProperty;
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
		 count(*) as numberOfFacts,
		 avg(f.factMeasure) as avgMeasure
	into #result_yit
	from Fact_SCD2 f
	join pitDimension_SCD2('2014-01-01') pit
	  on pit.dim_ID = f.dim_ID
	where f.factDate between '2014-01-01' and '2014-12-31'		   
	group by pit.dimProperty;
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
		 d.dimProperty,
		 count(*) as numberOfFacts,
		 avg(f.factMeasure) as avgMeasure
	into #result_toy
	from Fact_SCD2 f
	join Dimension_SCD2 d
	  on d.dim_ID = f.dim_ID
	where f.factDate between '2014-01-01' and '2014-12-31'	   
	group by d.dimProperty;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_toy;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'TOY' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;

	if @updateStatistics = 1 
	begin
		update statistics Dimension_SCD2 with FULLSCAN;
		update statistics Fact_SCD2 with FULLSCAN;
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



