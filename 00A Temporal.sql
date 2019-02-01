-------------------------------------------------------------------------------------------------------------
--------------------------------------------- PARTITION SCHEME ----------------------------------------------
drop table if exists Fact;
drop table if exists Dimension_Mutable;
drop table if exists Dimension_Immutable;

begin try drop partition scheme Yearly; end try begin catch end catch
begin try drop partition function Yearly; end try begin catch end catch

create partition function Yearly (smalldatetime)
as range right for values (
	'20000101', '20010101', '20020101', '20030101', '20040101', 
	'20050101', '20060101', '20070101', '20080101', '20090101',
	'20100101', '20110101', '20120101', '20130101', '20140101',
	'20150101', '20160101', '20170101', '20180101', '20190101',
	'20200101', '20210101', '20220101', '20230101', '20240101'
);

create partition scheme Yearly 
as partition Yearly all to ([PRIMARY]);

-------------------------------------------------------------------------------------------------------------
-------------------------------------------- TEMPORAL DIMENSION ---------------------------------------------
create table Dimension_Immutable (
	dim_ID int not null,
	primary key (dim_ID asc)
);

create table Dimension_Mutable (
	dim_ID int not null,
	dim_ValidFrom smalldatetime not null,
	dimProperty char(42) not null,
	foreign key (dim_ID) references Dimension_Immutable (dim_ID),
	primary key (dim_ID asc, dim_ValidFrom desc)
) on Yearly(dim_ValidFrom);

create table Fact (
	dim_ID int not null, 
	factDate smalldatetime not null,
	factMeasure smallmoney not null,
	foreign key (dim_ID) references Dimension_Immutable (dim_ID),
	primary key (dim_ID asc, factDate asc)
) on Yearly(factDate);

-------------------------------------------------------------------------------------------------------------
----------------------------------------- POPULATING THE DIMENSION ------------------------------------------
-- ~ 5 minutes loading time
-------------------------------------------------------------------------------------------------------------
declare @numberOfUniques int = power(2, 20);
with idGen as (
	select 1 as id
	union all
	select id + 1 from idGen where id < @numberOfUniques
)
insert into Dimension_Immutable (dim_ID)
select id from idGen
option (MAXRECURSION 0);
-- select count(*) from Dimension_Immutable;

-- truncate table Dimension_Mutable;
declare @changeEveryNumberOfMinutes int = 5; -- every five minutes
with versions as (
	select 
		dim_Id, 
		case 
			when dim_ID = 1                                   then power(2, 20)
			when dim_ID between power(2, 1)  and power(2, 2)  then power(2, 19) 
			when dim_ID between power(2, 2)  and power(2, 3)  then power(2, 18) 
			when dim_ID between power(2, 3)  and power(2, 4)  then power(2, 17) 
			when dim_ID between power(2, 4)  and power(2, 5)  then power(2, 16) 
			when dim_ID between power(2, 5)  and power(2, 6)  then power(2, 15) 
			when dim_ID between power(2, 6)  and power(2, 7)  then power(2, 14) 
			when dim_ID between power(2, 7)  and power(2, 8)  then power(2, 13) 
			when dim_ID between power(2, 8)  and power(2, 9)  then power(2, 12) 
			when dim_ID between power(2, 9)  and power(2, 10) then power(2, 11) 
			when dim_ID between power(2, 10) and power(2, 11) then power(2, 10) 
			when dim_ID between power(2, 11) and power(2, 12) then power(2, 9) 
			when dim_ID between power(2, 12) and power(2, 13) then power(2, 8) 
			when dim_ID between power(2, 13) and power(2, 14) then power(2, 7) 
			when dim_ID between power(2, 14) and power(2, 15) then power(2, 6) 
			when dim_ID between power(2, 15) and power(2, 16) then power(2, 5) 
			when dim_ID between power(2, 16) and power(2, 17) then power(2, 4) 
			when dim_ID between power(2, 17) and power(2, 18) then power(2, 3) 
			when dim_ID between power(2, 18) and power(2, 19) then power(2, 2) 
			when dim_ID between power(2, 19) and power(2, 20) then power(2, 1) 
			else 1 -- n/a
		end as NumberOfVersions
	from Dimension_Immutable
),
versioned_rows as (
	select dim_ID, NumberOfVersions - 1 as CurrentVersion, NumberOfVersions,
	       dateadd(minute, -power(2, 20) * @changeEveryNumberOfMinutes, '2018-01-01') as ValidFrom
	from versions
	union all 
	select dim_ID, CurrentVersion - 1, NumberOfVersions,
	       dateadd(minute, (-1E0 * CurrentVersion / NumberOfVersions) * power(2, 20) * @changeEveryNumberOfMinutes, '2018-01-01') as ValidFrom
	from versioned_rows where CurrentVersion > 0
)
insert into Dimension_Mutable (dim_ID, dim_ValidFrom, dimProperty)
select dim_ID, ValidFrom, 
      'dimProperty value ' + cast(dim_ID as varchar(10)) + ' since ' + convert(char(10), ValidFrom, 121) 
from versioned_rows
option (MAXRECURSION 0);
-- select count(distinct dim_ID), count(*), min(dim_ValidFrom), max(dim_ValidFrom) from Dimension_Mutable;
--        1 048 576             21 495 808   2008-01-13 02:40    2017-12-31 23:55

-- select datediff(minute, '2008-01-13 02:40', '2017-12-31 23:55');
-- 5 242 875
-------------------------------------------------------------------------------------------------------------
------------------------------------------- POPULATING THE FACTS --------------------------------------------
-- ~ 5 minutes loading time
-------------------------------------------------------------------------------------------------------------
-- truncate table Fact;
declare @firstDate smalldatetime = (
	select min(dim_ValidFrom) from Dimension_Mutable
); 
declare @lastDate smalldatetime = (
	select max(dim_ValidFrom) from Dimension_Mutable
); 
declare @dimSize int = (
	select count(*) from Dimension_Immutable
);

with minutely as (
	select @firstDate as factDate
	union all 
	select dateadd(minute, 1, factDate) from minutely
	where factDate < @lastDate
), 
ten as (
	select top 10 dim_ID 
	from Dimension_Immutable 
	order by dim_ID
),
veryLikelyTenRandomPerMinute as (
	select distinct
		-- inversely squarely proportional towards dim_ID, 
		-- skews distribution of facts towards dimension members with more versions
		1 + cast(square(rand(checksum(newid()))) * @dimSize as int) as dim_ID,
		m.factDate
	from minutely m
	cross apply ten t
)
insert into Fact (dim_ID, factDate, factMeasure)
select dim_ID, factDate, 1E0 * dim_ID / 100 
from veryLikelyTenRandomPerMinute
option (MAXRECURSION 0);
go
-- select count(distinct dim_ID), count(*), min(factDate),    max(factDate) from Fact;
--               1 048 576      52 427 836	2008-01-13 02:40  2017-12-31 23:55

-------------------------------------------------------------------------------------------------------------
------------------------------------ REDUCE INDEX FRAGMENTATION ---------------------------------------------
ALTER INDEX ALL ON Dimension_Immutable REBUILD;
ALTER INDEX ALL ON Dimension_Mutable REBUILD;
ALTER INDEX ALL ON Fact REBUILD;

/*
SELECT a.index_id, name, avg_fragmentation_in_percent  
FROM sys.dm_db_index_physical_stats (DB_ID(), 
      OBJECT_ID(N'Fact'), NULL, NULL, NULL) AS a  
    JOIN sys.indexes AS b 
      ON a.object_id = b.object_id AND a.index_id = b.index_id;  
*/

-------------------------------------------------------------------------------------------------------------
----------------------------- CREATING A POINT-IN-TIME PARAMETRIZED VIEW ------------------------------------
drop function if exists pitDimension;
go
-- select count(*) from pitDimension('2012-01-01');
create function pitDimension (
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
		Dimension_Mutable 
	where 
		dim_ValidFrom <= @timepoint
) dm_in_effect
where
	dm_in_effect.ReversedVersion = 1;
go

-------------------------------------------------------------------------------------------------------------
-------------------------------- CREATING A TWINING PARAMETRIZED VIEW ---------------------------------------
drop function if exists twineFact;
go

create function twineFact (
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
			dbo.Fact
		where
			factDate between @fromTimepoint and @toTimepoint	
		union all
		select
			dim_ID,
			dim_ValidFrom as Timepoint,
			'D' as Timeline
		from
			dbo.Dimension_Mutable
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

declare @model varchar(42) = 'Temporal';
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
			Fact
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		pitDimension('2018-01-01') in_effect
	on 
		in_effect.dim_ID = f.dim_ID;
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
		in_effect.dimProperty,
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
			Fact
		where 
			factDate between '2014-01-01' and '2014-12-31'	
		group by 
			dim_ID
	) f
	join
		pitDimension('2014-01-01') in_effect
	on 
		in_effect.dim_ID = f.dim_ID;
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
		twineFact('2014-01-01', '2014-12-31') in_effect
	join
		Fact f
	on
		f.dim_ID = in_effect.dim_ID 
	and
		f.factDate = in_effect.factDate	
	join
		Dimension_Mutable dm
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


