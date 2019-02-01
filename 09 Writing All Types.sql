/*
	Script for experimenting with loading data. 

	2018-07-22	CREATED

	Running time: ~1 hour and 10 minutes (on a fast server)
*/
-------------------------------------------------------------------------------------------------------------
-------------------------------------------- CREATE SOURCE DATA ---------------------------------------------
declare @maxId int = (
	select max(dim_ID) from Dimension_Immutable
);
declare @validFrom smalldatetime = '2018-01-01 00:00';

drop table if exists NewData_Dimension;
select
	*
into
	NewData_Dimension
from (
	-- changed values (all change)
	select
		dim_ID,
		@validFrom as dim_ValidFrom,
		'dimProperty value ' + cast(dim_ID as varchar(10)) + ' since ' + convert(char(10), @validFrom, 121) as dimProperty
	from
		Dimension_Immutable
	union all
	-- new values (equal amount of new values)
	select
		@maxId + dim_ID,
		@validFrom as dim_ValidFrom,
		'dimProperty value ' + cast(@maxId + dim_ID as varchar(10)) + ' since ' + convert(char(10), @validFrom, 121) as dimProperty
	from
		Dimension_Immutable
) w;

create unique clustered index pk_NewData_Dimension on NewData_Dimension (dim_ID asc, dim_ValidFrom desc);

-- one fact per dimension member
drop table if exists NewData_Fact;
select
	dim_ID,
	dim_ValidFrom as factDate,
	1E0 * dim_ID / 100 as factMeasure
into
	NewData_Fact
from 
	NewData_Dimension;

create unique clustered index pk_NewData_Fact on NewData_Fact (dim_ID asc, factDate asc);

-------------------------------------------------------------------------------------------------------------
-------------------------------------------- PERFORM THE TESTING --------------------------------------------
declare @writeRuns int = 3; 
declare @runs int;
declare @DB_ID int = DB_ID();
declare @model varchar(42);

if OBJECT_ID('Timings') is null
begin
	create table Timings (
		model varchar(42) not null,
		run int not null,
		query char(3) not null,
		executionTime int not null
	);
end

declare @startingTime_D datetime2(7);
declare @endingTime_D datetime2(7);
declare @startingTime_F datetime2(7);
declare @endingTime_F datetime2(7);

set nocount on;

------------------------------------------
set @model = 'Temporal';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	insert into Dimension_Immutable (dim_ID)
	select src.dim_ID
	from (select distinct dim_ID from NewData_Dimension) src
	left join Dimension_Immutable di
	on di.dim_ID = src.dim_ID
	where di.dim_ID is null;

	insert into Dimension_Mutable (dim_ID, dim_ValidFrom, dimProperty)
	select src.dim_ID, src.dim_ValidFrom, src.dimProperty
	from NewData_Dimension src
	left join Dimension_Mutable dm
	on dm.dim_ID = src.dim_ID and dm.dim_ValidFrom = src.dim_ValidFrom
	where dm.dim_ID is null;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact (dim_ID, factDate, factMeasure)
	select src.dim_ID, src.factDate, src.factMeasure
	from NewData_Fact src
	left join Fact f
	on f.dim_ID = src.dim_ID and f.factDate = src.factDate
	where f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Optional';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	insert into Dimension_Immutable (dim_ID)
	select src.dim_ID
	from (select distinct dim_ID from NewData_Dimension) src
	left join Dimension_Immutable di
	on di.dim_ID = src.dim_ID
	where di.dim_ID is null;

	insert into Dimension_Mutable (dim_ID, dim_ValidFrom, dimProperty)
	select src.dim_ID, src.dim_ValidFrom, src.dimProperty
	from NewData_Dimension src
	left join Dimension_Mutable dm
	on dm.dim_ID = src.dim_ID and dm.dim_ValidFrom = src.dim_ValidFrom
	where dm.dim_ID is null;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_Optional (dim_ID, factDate, dim_ValidFrom, factMeasure)
	select src.dim_ID, src.factDate, src.factDate, src.factMeasure
	from ( 
		select 
			twine.dim_ID,
			twine.Timepoint as factDate,
			twine.Timeline,
			twine.factMeasure,
			MAX(case when Timeline = 'D' then Timepoint end) over (
				partition by dim_ID order by Timepoint
			) as dim_ValidFrom
		from (
			select
				dim_ID,
				factMeasure,
				factDate as Timepoint,
				'F' as Timeline
			from 
				NewData_Fact
			union all
			select
				dim_ID,
				null as factMeasure,
				dim_ValidFrom as Timepoint,
				'D' as Timeline
			from
				Dimension_Mutable
		) twine		
	) src
	left join Fact_Optional f
	on f.dim_ID = src.dim_ID and f.factDate = src.factDate
	where f.dim_ID is null
	and src.Timeline = 'F';
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Type 1';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	merge Dimension_SCD1 d
	using NewData_Dimension src
	   on src.dim_ID = d.dim_ID
	when not matched then insert (dim_ID, dimProperty)
	values (src.dim_ID, src.dimProperty)
	when matched then update
	set d.dimProperty = src.dimProperty;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_SCD1 (dim_ID, factDate, factMeasure)
	select src.dim_ID, src.factDate, src.factMeasure
	from NewData_Fact src
	left join Fact_SCD1 f
	on f.dim_ID = src.dim_ID and f.factDate = src.factDate
	where f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Type 2';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	insert into Dimension_SCD2 (dim_ValidFrom, dimStatic, dimProperty)
	select src.dim_ValidFrom, src.dim_ID, src.dimProperty
	from NewData_Dimension src
	left join Dimension_SCD2 d
	on d.dimStatic = src.dim_ID and d.dim_ValidFrom = src.dim_ValidFrom
	where d.dim_ID is null;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_SCD2 (dim_ID, factDate, factMeasure)
	select d.dim_ID, src.factDate, src.factMeasure
	from ( 
		select 
			twine.dimStatic,
			twine.Timepoint as factDate,
			twine.Timeline,
			twine.factMeasure,
			MAX(case when Timeline = 'D' then Timepoint end) over (
				partition by dimStatic order by Timepoint
			) as dim_ValidFrom
		from (
			select
				dim_ID as dimStatic,
				factDate as Timepoint,
				factMeasure,
				'F' as Timeline
			from 
				NewData_Fact
			union all
			select
				dimStatic,
				dim_ValidFrom as Timepoint,
				null as factMeasure,
				'D' as Timeline
			from
				Dimension_SCD2
		) twine		
	) src
	join Dimension_SCD2 d
	  on d.dimStatic = src.dimStatic and d.dim_ValidFrom = src.dim_ValidFrom
	left join Fact_SCD2 f
	on f.dim_ID = d.dim_ID and f.factDate = src.factDate
	where src.Timeline = 'F'  
	  and f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Type 3';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	merge Dimension_SCD3 d
	using NewData_Dimension src
	   on src.dim_ID = d.dim_ID
	when not matched then insert (dim_ID, dimProperty_V1, dimProperty_V2)
	values (src.dim_ID, src.dimProperty, src.dimProperty)
	when matched then update
	set d.dimProperty_V2 = d.dimProperty_V1,
		d.dimProperty_V1 = src.dimProperty;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_SCD3 (dim_ID, factDate, factMeasure)
	select src.dim_ID, src.factDate, src.factMeasure
	from NewData_Fact src
	left join Fact_SCD3 f
	on f.dim_ID = src.dim_ID and f.factDate = src.factDate
	where f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Type 4';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	insert into Dimension_SCD4 (dim_ID, dim_ValidFrom, dimProperty)
	select
		dim_ID,
		dim_ValidFrom,
		dimProperty
	from ( -- utilize the fact that you can select from a merge having an output
		merge Dimension_Current_SCD4 d
		using NewData_Dimension src
		   on src.dim_ID = d.dim_ID
		when not matched then insert (dim_ID, dim_ValidFrom, dimProperty)
		values (src.dim_ID, src.dim_ValidFrom, src.dimProperty)
		when matched then update
		set d.dimProperty = src.dimProperty, d.dim_ValidFrom = src.dim_ValidFrom
		output $action as Op, deleted.dim_ID, deleted.dim_ValidFrom, deleted.dimProperty 
	) m
	where
		m.Op = 'UPDATE';
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_SCD4 (dim_ID, factDate, factMeasure)
	select src.dim_ID, src.factDate, src.factMeasure
	from NewData_Fact src
	left join Fact_SCD4 f
	on f.dim_ID = src.dim_ID and f.factDate = src.factDate
	where f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Type 5';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	BEGIN TRANSACTION;
	-- we find the last value outside of the timings (as it had not been necessary
	-- if dim_ID had been declared as identity(1,1))
	declare @max_dim_ID5 int = (select max(dim_ID) from Dimension_SCD5);

	set @startingTime_D = SYSDATETIME();
	insert into Dimension_SCD5 (dim_ID, dim_ValidFrom, dimStatic, dimProperty, dim_Current_ID)
	select @max_dim_ID5 + src.dim_ID, src.dim_ValidFrom, src.dim_ID, src.dimProperty, src.dim_ID
	from NewData_Dimension src
	left join Dimension_SCD5 d
	on d.dimStatic = src.dim_ID and d.dim_ValidFrom = src.dim_ValidFrom
	where d.dim_ID is null;

	update d
	set d.dim_Current_ID = d_in_effect.dim_ID
	from Dimension_SCD5 d
	join pitDimension_SCD5('2018-01-01') pit
	  on pit.dim_ID = d.dim_ID
	join Dimension_SCD5 d_in_effect
	  on d_in_effect.dimStatic = pit.dimStatic
	 and d_in_effect.dim_ValidFrom = pit.dim_ValidFrom;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_SCD5 (dim_ID, factDate, factMeasure)
	select d.dim_ID, src.factDate, src.factMeasure
	from ( 
		select 
			twine.dimStatic,
			twine.Timepoint as factDate,
			twine.Timeline,
			twine.factMeasure,
			MAX(case when Timeline = 'D' then Timepoint end) over (
				partition by dimStatic order by Timepoint
			) as dim_ValidFrom
		from (
			select
				dim_ID as dimStatic,
				factDate as Timepoint,
				factMeasure,
				'F' as Timeline
			from 
				NewData_Fact
			union all
			select
				dimStatic,
				dim_ValidFrom as Timepoint,
				null as factMeasure,
				'D' as Timeline
			from
				Dimension_SCD5
		) twine		
	) src
	join Dimension_SCD5 d
	  on d.dimStatic = src.dimStatic and d.dim_ValidFrom = src.dim_ValidFrom
	left join Fact_SCD5 f
	on f.dim_ID = d.dim_ID and f.factDate = src.factDate
	where src.Timeline = 'F'  
	  and f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Type 6';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	insert into Dimension_SCD6 (dim_ID, dim_ValidFrom, dimProperty)
	select src.dim_ID, src.dim_ValidFrom, src.dimProperty
	from NewData_Dimension src
	left join Dimension_SCD6 dm
	on dm.dim_ID = src.dim_ID and dm.dim_ValidFrom = src.dim_ValidFrom
	where dm.dim_ID is null;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_SCD6 (dim_ID, factDate, factMeasure)
	select src.dim_ID, src.factDate, src.factMeasure
	from NewData_Fact src
	left join Fact_SCD6 f
	on f.dim_ID = src.dim_ID and f.factDate = src.factDate
	where f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Type 7';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	-- we find the last value outside of the timings (as it had not been necessary
	-- if dim_ID had been declared as identity(1,1))
	declare @max_dim_ID7 int = (select max(dim_ID) from Dimension_SCD5);
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	insert into Dimension_SCD7 (dim_ID, dim_ValidFrom, dimStatic, dimProperty)
	select @max_dim_ID7 + src.dim_ID, src.dim_ValidFrom, src.dim_ID, src.dimProperty
	from NewData_Dimension src
	left join Dimension_SCD7 d
	on d.dimStatic = src.dim_ID and d.dim_ValidFrom = src.dim_ValidFrom
	where d.dim_ID is null;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	update f
	set f.dim_Current_ID = d_in_effect.dim_ID
	from Fact_SCD7 f
	join pitDimension_SCD7('2018-01-01') pit
	  on pit.dim_ID = f.dim_ID
	join Dimension_SCD7 d_in_effect
	  on d_in_effect.dimStatic = pit.dimStatic and d_in_effect.dim_ValidFrom = pit.dim_ValidFrom;

	insert into Fact_SCD7 (dim_ID, factDate, factMeasure, dim_Current_ID)
	select d.dim_ID, src.factDate, src.factMeasure, d.dim_ID
	from ( 
		select 
			twine.dimStatic,
			twine.Timepoint as factDate,
			twine.Timeline,
			twine.factMeasure,
			MAX(case when Timeline = 'D' then Timepoint end) over (
				partition by dimStatic order by Timepoint
			) as dim_ValidFrom
		from (
			select
				dim_ID as dimStatic,
				factDate as Timepoint,
				factMeasure,
				'F' as Timeline
			from 
				NewData_Fact
			union all
			select
				dimStatic,
				dim_ValidFrom as Timepoint,
				null as factMeasure,
				'D' as Timeline
			from
				Dimension_SCD7
		) twine		
	) src
	join Dimension_SCD7 d
	  on d.dimStatic = src.dimStatic and d.dim_ValidFrom = src.dim_ValidFrom
	left join Fact_SCD7 f
	on f.dim_ID = d.dim_ID and f.factDate = src.factDate
	where src.Timeline = 'F'  
	  and f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end

------------------------------------------
set @model = 'Junk';
delete Timings where model = @model and query in ('w/D', 'w/F');

set @runs = @writeRuns;
while(@runs > 0) 
begin
	-- clear all caches
	DBCC FREESYSTEMCACHE('ALL');
	DBCC FREESESSIONCACHE;
	DBCC FREEPROCCACHE;
	DBCC FLUSHPROCINDB(@DB_ID);
	CHECKPOINT;
	DBCC DROPCLEANBUFFERS;
	-- we find the last value outside of the timings (as it had not been necessary
	-- if dim_Junk_ID had been declared as identity(1,1))
	declare @max_dim_IDJ int = (select max(dim_Junk_ID) from Dimension_Junk_RCD);	
	
	BEGIN TRANSACTION;
	set @startingTime_D = SYSDATETIME();
	insert into Dimension_Junk_SCD (dim_ID)
	select src.dim_ID
	from (select distinct dim_ID from NewData_Dimension) src
	left join Dimension_Junk_SCD jscd
	on jscd.dim_ID = src.dim_ID
	where jscd.dim_ID is null;

	insert into Dimension_Junk_RCD (dim_Junk_ID, dimProperty)
	select @max_dim_IDJ + src.dim_ID, src.dimProperty
	from NewData_Dimension src
	left join Dimension_Junk_RCD jrcd
	on jrcd.dimProperty = src.dimProperty
	where jrcd.dim_Junk_ID is null;

	insert into Dimension_Junk_Mini (dim_ID, dim_Junk_ID, dim_ValidFrom) 
	select src.dim_ID, jrcd.dim_Junk_ID, src.dim_ValidFrom
	from NewData_Dimension src
	join Dimension_Junk_RCD jrcd
	on jrcd.dimProperty = src.dimProperty
	left join Dimension_Junk_Mini jm
	on jm.dim_ID = src.dim_ID
	and jm.dim_Junk_ID = jrcd.dim_Junk_ID
	and jm.dim_ValidFrom = src.dim_ValidFrom
	where jm.dim_ID is null;
	set @endingTime_D = SYSDATETIME();

	set @startingTime_F = SYSDATETIME();
	insert into Fact_Junk (dim_ID, factDate, factMeasure)
	select src.dim_ID, src.factDate, src.factMeasure
	from NewData_Fact src
	left join Fact_Junk f
	on f.dim_ID = src.dim_ID and f.factDate = src.factDate
	where f.dim_ID is null;
	set @endingTime_F = SYSDATETIME();
	ROLLBACK;

	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/D' , datediff(ms, @startingTime_D, @endingTime_D)
	insert into Timings (model, run, query, executionTime) 
	select @model, @runs, 'w/F' , datediff(ms, @startingTime_F, @endingTime_F)

	set @runs = @runs - 1;
end



----------------------------------------------------------------------------------------------
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
