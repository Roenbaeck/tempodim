drop table if exists Fact_SCD7;
drop table if exists Dimension_SCD7;

-------------------------------------------------------------------------------------------------------------
------------------------------------------------ SCD Type 7 -------------------------------------------------
create table Dimension_SCD7 (
	dim_ID int not null, -- needs to be a larger data type now
	dim_ValidFrom smalldatetime not null,
	dimStatic int not null, -- we will put the dim_ID from Dimension_Immutable here
	dimProperty char(42) not null,
	primary key (dim_ID asc),
	unique (dimStatic asc, dim_ValidFrom desc) -- necessary for performance reasons
); -- note that partitioning is not possible since dim_ValidFrom is not part of the primary key

create table Fact_SCD7 (
	dim_ID int not null, 
	factDate smalldatetime not null,
	dim_Current_ID int not null,
	factMeasure smallmoney not null,
	foreign key (dim_ID) references Dimension_SCD7 (dim_ID),
	foreign key (dim_Current_ID) references Dimension_SCD7 (dim_ID),
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

-------------------------------------------------------------------------------------------------------------
----------------------------------------- POPULATING THE DIMENSION ------------------------------------------
-- ~ 30 seconds loading time
-------------------------------------------------------------------------------------------------------------
insert into Dimension_SCD7 (dim_ID, dim_ValidFrom, dimStatic, dimProperty)
select
	scd2.dim_ID,
	scd2.dim_ValidFrom,
	scd2.dimStatic,
	scd2.dimProperty
from
	Dimension_SCD2 scd2;

-- select count(distinct dimStatic), count(*), min(dim_ValidFrom), max(dim_ValidFrom) from Dimension_SCD7;
--        1 048 576                21 495 808   2008-01-13 02:40    2017-12-31 23:55

-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
-- ~ 3 minutes loading time
-------------------------------------------------------------------------------------------------------------
-- truncate table Fact_SCD7;
insert into Fact_SCD7 (dim_ID, factDate, dim_Current_ID, factMeasure)
select 
	f.dim_ID,
	f.factDate,
	d_in_effect.dim_ID,
	f.factMeasure
from -- select count(*), count(distinct f.dim_ID) from
	Fact_SCD2 f
join
	pitDimension_SCD2('2018-01-01') pit
on
	pit.dim_ID = f.dim_ID
join
	Dimension_SCD2 d_in_effect
on
	d_in_effect.dimStatic = pit.dimStatic
and
	d_in_effect.dim_ValidFrom = pit.dim_ValidFrom;

-- select count(*), count(distinct dim_ID), count(distinct dim_Current_ID) from Fact_SCD7;
-- 52427789	10373753	1048576

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Dimension_SCD7 REBUILD;
ALTER INDEX ALL ON Fact_SCD7 REBUILD;

-------------------------------------------------------------------------------------------------------------
----------------------------- CREATING A POINT-IN-TIME PARAMETRIZED VIEW ------------------------------------
drop function if exists pitDimension_SCD7;
go
create function pitDimension_SCD7 (
	@timepoint smalldatetime
) 
returns table as return
select
	d.dim_ID,
	dm_in_effect.dim_ValidFrom,
	dm_in_effect.dimStatic,
	dm_in_effect.dimProperty
from 
	Dimension_SCD7 d
cross apply (
	select top 1 
		s.*
	from 
		Dimension_SCD7 s
	where
		s.dimStatic = d.dimStatic
	and
		s.dim_ValidFrom <= @timepoint
	order by
		s.dim_ValidFrom desc
) dm_in_effect;
go

-- select count(*), count(distinct dimStatic) from pitDimension_SCD7('20180101')

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

declare @model varchar(42) = 'Type 7';
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
		 in_effect.dimProperty,
		 count(*) as numberOfFacts,
		 avg(f.factMeasure) as avgMeasure
	into #result_tiy
	from Fact_SCD7 f
	join Dimension_SCD7 in_effect
	  on in_effect.dim_ID = f.dim_Current_ID
	where f.factDate between '2014-01-01' and '2014-12-31'		   
	group by in_effect.dimProperty;
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
	from Fact_SCD7 f
	join pitDimension_SCD7('2014-01-01') pit
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
	from Fact_SCD7 f
	join Dimension_SCD7 d
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
		update statistics Dimension_SCD7 with FULLSCAN;
		update statistics Fact_SCD7 with FULLSCAN;
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



