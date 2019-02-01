drop table if exists Fact_SCD3;
drop table if exists Dimension_SCD3;

-------------------------------------------------------------------------------------------------------------
------------------------------------------------ SCD Type 3 -------------------------------------------------
create table Dimension_SCD3 (
	dim_ID int not null,
	dimProperty_V1 char(42) not null,
	dimProperty_V2 char(42) not null,
	primary key (dim_ID asc)
);

create table Fact_SCD3 (
	dim_ID int not null, 
	factDate smalldatetime not null,
	factMeasure smallmoney not null,
	foreign key (dim_ID) references Dimension_SCD3 (dim_ID),
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

-------------------------------------------------------------------------------------------------------------
----------------------------------------- POPULATING THE DIMENSION ------------------------------------------
-- truncate table Dimension_SCD3;
insert into Dimension_SCD3 (dim_ID, dimProperty_V1, dimProperty_V2)
select d_current.dim_ID, d_current.dimProperty, d_historic.dimProperty
from pitDimension('20180101') d_current
join pitDimension('20140101') d_historic
on d_historic.dim_ID = d_current.dim_ID;

-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
insert into Fact_SCD3 select * from Fact;
-- select count(*) from Fact_SCD3;

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Dimension_SCD3 REBUILD;
ALTER INDEX ALL ON Fact_SCD3 REBUILD;


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

declare @model varchar(42) = 'Type 3';
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
		 d.dimProperty_V1,
		 f.numberOfFacts,
		 f.avgMeasure
	into #result_tiy
	from (
		select
			dim_ID,
			count(*) as numberOfFacts,
			avg(factMeasure) as avgMeasure
		from			 
			Fact_SCD3 
		where
			factDate between '2014-01-01' and '2014-12-31'
		group by
			dim_ID
	) f
	join Dimension_SCD3 d
	  on d.dim_ID = f.dim_ID;
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
		 d.dimProperty_V2,
		 f.numberOfFacts,
		 f.avgMeasure
	into #result_yit
	from (
		select
			dim_ID,
			count(*) as numberOfFacts,
			avg(factMeasure) as avgMeasure
		from			 
			Fact_SCD3 
		where
			factDate between '2014-01-01' and '2014-12-31'
		group by
			dim_ID
	) f
	join Dimension_SCD3 d
	  on d.dim_ID = f.dim_ID;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_yit;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'YIT' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;

	if @updateStatistics = 1 
	begin
		update statistics Dimension_SCD3 with FULLSCAN;
		update statistics Fact_SCD3 with FULLSCAN;
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






