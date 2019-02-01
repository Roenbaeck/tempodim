drop table if exists Fact_SCD1;
drop table if exists Dimension_SCD1;

-------------------------------------------------------------------------------------------------------------
------------------------------------------------ SCD Type 1 -------------------------------------------------
create table Dimension_SCD1 (
	dim_ID int not null,
	dimProperty char(42) not null,
	primary key (dim_ID asc)
);

create table Fact_SCD1 (
	dim_ID int not null, 
	factDate smalldatetime not null,
	factMeasure smallmoney not null,
	foreign key (dim_ID) references Dimension_SCD1 (dim_ID),
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

-------------------------------------------------------------------------------------------------------------
----------------------------------------- POPULATING THE DIMENSION ------------------------------------------
insert into Dimension_SCD1 (dim_ID, dimProperty)
select dim_ID, dimProperty
from pitDimension('20180101');
-- select count(*) from Dimension_SCD1;
-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
insert into Fact_SCD1 select * from Fact;
-- select count(*) from Fact_SCD1;

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Dimension_SCD1 REBUILD;
ALTER INDEX ALL ON Fact_SCD1 REBUILD;


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

declare @model varchar(42) = 'Type 1';
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
		 d.dimProperty,
		 f.numberOfFacts,
		 f.avgMeasure
	into #result_tiy
	from (
		select
			dim_ID,
			count(*) as numberOfFacts,
			avg(factMeasure) as avgMeasure
		from			 
			Fact_SCD1 
		where
			factDate between '2014-01-01' and '2014-12-31'
		group by
			dim_ID
	) f
	join Dimension_SCD1 d
	  on d.dim_ID = f.dim_ID;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_tiy;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'TIY' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;

	if @updateStatistics = 1 
	begin
		update statistics Dimension_SCD1 with FULLSCAN;
		update statistics Fact_SCD1 with FULLSCAN;
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






