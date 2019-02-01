drop table if exists Fact_SCD4;
drop table if exists Dimension_SCD4;
drop table if exists Dimension_Current_SCD4;

-------------------------------------------------------------------------------------------------------------
------------------------------------------------ SCD Type 4 -------------------------------------------------
create table Dimension_Current_SCD4 (
	dim_ID int not null,
	dim_ValidFrom smalldatetime not null,
	dimProperty char(42) not null,
	primary key (dim_ID asc)
);

create table Fact_SCD4 (
	dim_ID int not null, 
	factDate smalldatetime not null,
	factMeasure smallmoney not null,
	foreign key (dim_ID) references Dimension_Current_SCD4 (dim_ID),
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

create table Dimension_SCD4 (
	dim_ID int not null,
	dim_ValidFrom smalldatetime not null,
	dimProperty char(42) not null,
	primary key (dim_ID asc, dim_ValidFrom desc)
) on Yearly(dim_ValidFrom);

-------------------------------------------------------------------------------------------------------------
---------------------------------------- POPULATING THE DIMENSIONS ------------------------------------------
insert into Dimension_Current_SCD4 (dim_ID, dim_ValidFrom, dimProperty)
select dim_ID, dim_ValidFrom, dimProperty
from pitDimension('20180101');

-- insert all other history 
insert into Dimension_SCD4 (dim_ID, dim_ValidFrom, dimProperty)
select dm.dim_ID, dm.dim_ValidFrom, dm.dimProperty
from Dimension_Mutable dm
left join Dimension_Current_SCD4 cur
on cur.dim_ID = dm.dim_ID and cur.dim_ValidFrom = dm.dim_ValidFrom
where cur.dim_ID is null;

-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
insert into Fact_SCD4 select * from Fact;
-- select count(*) from Fact_SCD4;

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Dimension_Current_SCD4 REBUILD;
ALTER INDEX ALL ON Dimension_SCD4 REBUILD;
ALTER INDEX ALL ON Fact_SCD4 REBUILD;


-------------------------------------------------------------------------------------------------------------
----------------------------- CREATING A POINT-IN-TIME PARAMETRIZED VIEW ------------------------------------
drop function if exists pitDimension_SCD4;
go
-- select count(*) from pitDimension('2012-01-01');
create function pitDimension_SCD4 (
	@timepoint smalldatetime
) 
returns table as return
select
	di.dim_ID,
	dm_in_effect.dim_ValidFrom,
	dm_in_effect.dimProperty
from 
	Dimension_Current_SCD4 di
cross apply (
	select top 1 
		dm.dim_ID,
		dm.dim_ValidFrom,
		dm.dimProperty
	from (
		select * from Dimension_Current_SCD4
		union all
		select * from Dimension_SCD4
	) dm
	where
		dm.dim_ID = di.dim_ID
	and
		dm.dim_ValidFrom <= @timepoint
	order by
		dm.dim_ValidFrom desc
) dm_in_effect;
go

-------------------------------------------------------------------------------------------------------------
-------------------------------- CREATING A TWINING PARAMETRIZED VIEW ---------------------------------------
drop function if exists twineFact_SCD4;
go

create function twineFact_SCD4 (
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
			dbo.Fact_SCD4
		where
			factDate between @fromTimepoint and @toTimepoint	
		union all
		select
			dim_ID,
			dim_ValidFrom as Timepoint,
			'D' as Timeline
		from (
			select * from Dimension_Current_SCD4
			union all
			select * from Dimension_SCD4
		) d
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

declare @model varchar(42) = 'Type 4';
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
		dc.dimProperty,
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
			Fact_SCD4
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		Dimension_Current_SCD4 dc
	on 
		dc.dim_ID = f.dim_ID;
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
			Fact_SCD4
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		pitDimension_SCD4('2014-01-01') pit
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
		d.dimProperty,
		count(*) as numberOfFacts,
		avg(f.factMeasure) as avgMeasure
	into
		#result_toy
	from 
		twineFact_SCD4('2014-01-01', '2014-12-31') in_effect
	join
		Fact_SCD4 f
	on
		f.dim_ID = in_effect.dim_ID 
	and
		f.factDate = in_effect.factDate	
	join (
		select * from Dimension_Current_SCD4
		union all
		select * from Dimension_SCD4
	) d
	on
		d.dim_ID = in_effect.dim_ID
	and
		d.dim_ValidFrom = in_effect.dim_ValidFrom
	group by 
		d.dimProperty;
	set @endingTime = SYSDATETIME();
	drop table if exists #result_toy;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'TOY' , datediff(ms, @startingTime, @endingTime)
	where @updateStatistics = 0;
	
	if @updateStatistics = 1 
	begin
		update statistics Dimension_Immutable with FULLSCAN;
		update statistics Dimension_Mutable with FULLSCAN;
		update statistics Fact with FULLSCAN;
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






