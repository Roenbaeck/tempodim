-------------------------------------------------------------------------------------------------------------
--------------------------------------------- PARTITION SCHEME ----------------------------------------------
drop table if exists Fact_SCD6;
drop table if exists Dimension_SCD6;

-------------------------------------------------------------------------------------------------------------
------------------------------------------------ SCD Type 6 -------------------------------------------------
create table Dimension_SCD6 (
	dim_ID int not null,
	dim_ValidFrom smalldatetime not null,
	dimProperty char(42) not null,
	primary key (dim_ID asc, dim_ValidFrom desc)
) on Yearly(dim_ValidFrom);

create table Fact_SCD6 (
	dim_ID int not null, 
	factDate smalldatetime not null,
	factMeasure smallmoney not null,
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

-------------------------------------------------------------------------------------------------------------
----------------------------------------- POPULATING THE DIMENSION ------------------------------------------
insert into Dimension_SCD6 (dim_ID, dim_ValidFrom, dimProperty)
select dim_ID, dim_ValidFrom, dimProperty from Dimension_Mutable;

-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
-- ~ 30 seconds loading time
-------------------------------------------------------------------------------------------------------------
-- truncate table Fact_SCD6;
insert into Fact_SCD6 (dim_ID, factDate, factMeasure)
select dim_ID, factDate, factMeasure from Fact;

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Dimension_SCD6 REBUILD;
ALTER INDEX ALL ON Fact_SCD6 REBUILD;

-------------------------------------------------------------------------------------------------------------
----------------------------- CREATING A POINT-IN-TIME PARAMETRIZED VIEW ------------------------------------
drop function if exists pitDimension_SCD6;
go
-- select count(*) from pitDimension('2012-01-01');
create function pitDimension_SCD6 (
	@timepoint smalldatetime
) 
returns table as return
select
	dm_in_effect.dim_ID,
	dm_in_effect.dim_ValidFrom,
	dm_in_effect.dimProperty
from (
	select
		*,
		ROW_NUMBER() over (partition by dim_ID order by dim_ValidFrom desc) as ReversedVersion
	from 
		Dimension_SCD6 
	where 
		dim_ValidFrom <= @timepoint
) dm_in_effect
where
	dm_in_effect.ReversedVersion = 1;
go

/*
create function pitDimension_SCD6 (
	@timepoint smalldatetime
) 
returns table as return
select
	dm_in_effect.dim_ID,
	dm_in_effect.dim_ValidFrom,
	dm_in_effect.dimProperty
from 
	(select distinct dim_ID from Dimension_SCD6) di
cross apply (
	select top 1 
		dm.dim_ID,
		dm.dim_ValidFrom,
		dm.dimProperty
	from 
		Dimension_SCD6 dm
	where
		dm.dim_ID = di.dim_ID
	and
		dm.dim_ValidFrom <= @timepoint
	order by
		dm.dim_ValidFrom desc
) dm_in_effect;
go
*/
-------------------------------------------------------------------------------------------------------------
-------------------------------- CREATING A TWINING PARAMETRIZED VIEW ---------------------------------------
drop function if exists twineFact_SCD6;
go

create function twineFact_SCD6 (
	@fromTimepoint smalldatetime,
	@toTimepoint smalldatetime
) 
returns table as return
select 
	in_effect.dim_ID,
	in_effect.factDate,
	in_effect.dim_ValidFrom
from (
	select
		twine.dim_ID,
		twine.Timepoint as factDate,
		twine.Timeline,
		MAX(case when Timeline = 'D' then Timepoint end) over (
			partition by dim_ID order by Timepoint
		) as dim_ValidFrom
	from (
		select
			dim_ID,
			factDate as Timepoint,
			'F' as Timeline
		from 
			dbo.Fact_SCD6
		where
			factDate between @fromTimepoint and @toTimepoint	
		union all
		select
			dim_ID,
			dim_ValidFrom as Timepoint,
			'D' as Timeline
		from
			dbo.Dimension_SCD6
		where
			dim_ValidFrom <= @toTimepoint
	) twine	
) in_effect	
where
	in_effect.Timeline = 'F';
go

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

declare @model varchar(42) = 'Type 6';
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
			Fact_SCD6
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		pitDimension_SCD6('2018-01-01') pit
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
			Fact_SCD6
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		pitDimension_SCD6('2014-01-01') pit
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
	into
		#result_toy
	from 
		twineFact_SCD6('2014-01-01', '2014-12-31') in_effect
	join
		Fact_SCD6 f
	on
		f.dim_ID = in_effect.dim_ID 
	and
		f.factDate = in_effect.factDate	
	join
		Dimension_SCD6 dm
	on
		dm.dim_ID = in_effect.dim_ID
	and
		dm.dim_ValidFrom = in_effect.dim_ValidFrom
	group by 
		dm.dimProperty;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_toy;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'TOY' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;
	
	if @updateStatistics = 1 
	begin
		update statistics Dimension_SCD6 with FULLSCAN;
		update statistics Fact_SCD6 with FULLSCAN;
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


