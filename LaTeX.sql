-- number formats
declare @timesFormat varchar(10) = 'N1'; -- 1 decimal
declare @countFormat varchar(10) = 'N0'; -- no decimals

drop table if exists #results;

with results as (
	select
		rm.model,
		rm.query,
		round(rm.Median, 0) as Median
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
)
select
	*
into
	#results
from	
	results
pivot (
	max(Median) for model in (
		[Temporal], [Optional], [Type 1], [Type 2], [Type 3], [Type 4], [Type 5], [Type 6], [Type 7], [Junk]
	)
) pvt;

select
	case query 
		when 'TIY' then '\tiy/' 
		when 'YIT' then '\yit/' 
		when 'TOY' then '\toy/' 
		when 'w/D' then '{\footnotesize Dim}'
		when 'w/F' then '{\footnotesize Fact}'
	end + ' & ' +
	isnull(format(1E0 * [Temporal] / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Optional] / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Type 1]   / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Type 2]   / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Type 3]   / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Type 4]   / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Type 5]   / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Type 6]   / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Type 7]   / 1000, @timesFormat), '{\NA}') + ' & ' +
	isnull(format(1E0 * [Junk]     / 1000, @timesFormat), '{\NA}') + ' \\' as tabular
from -- select * from 
	#results
order by
	case query when 'TIY' then 1 when 'YIT' then 2 when 'TOY' then 3 when 'w/D' then 4 when 'w/F' then 5 end;

with waste as (
	select
		r.query,
		case when c.[Temporal] > 0 then c.[Temporal] / r.[Temporal] else c.[Temporal] / w.[Temporal] end as [Temporal],
		case when c.[Optional] > 0 then c.[Optional] / r.[Optional] else c.[Optional] / w.[Optional] end as [Optional],
		case when c.[Type 1]   > 0 then c.[Type 1]   / r.[Type 1]   else c.[Type 1]   / w.[Type 1]   end as [Type 1],
		case when c.[Type 2]   > 0 then c.[Type 2]   / r.[Type 2]   else c.[Type 2]   / w.[Type 2]   end as [Type 2],
		case when c.[Type 3]   > 0 then c.[Type 3]   / r.[Type 3]   else c.[Type 3]   / w.[Type 3]   end as [Type 3],
		case when c.[Type 4]   > 0 then c.[Type 4]   / r.[Type 4]   else c.[Type 4]   / w.[Type 4]   end as [Type 4],
		case when c.[Type 5]   > 0 then c.[Type 5]   / r.[Type 5]   else c.[Type 5]   / w.[Type 5]   end as [Type 5],
		case when c.[Type 6]   > 0 then c.[Type 6]   / r.[Type 6]   else c.[Type 6]   / w.[Type 6]   end as [Type 6],
		case when c.[Type 7]   > 0 then c.[Type 7]   / r.[Type 7]   else c.[Type 7]   / w.[Type 7]   end as [Type 7],
		case when c.[Junk]     > 0 then c.[Junk]     / r.[Junk]     else c.[Junk]     / w.[Junk]     end as [Junk]    
	from 
		#results r
	cross apply (
		select 
			sum([Temporal]) as [Temporal],
			sum([Optional]) as [Optional],
			sum([Type 1]) as [Type 1],
			sum([Type 2]) as [Type 2],
			sum([Type 3]) as [Type 3],
			sum([Type 4]) as [Type 4],
			sum([Type 5]) as [Type 5],
			sum([Type 6]) as [Type 6],
			sum([Type 7]) as [Type 7],
			sum([Junk]) as [Junk]    
		from 
			#results
		where
			query in ('w/D', 'w/F')
	) w
	cross apply (
		values (
			1E0 * (w.[Temporal] - r.[Temporal]),
			1E0 * (w.[Optional] - r.[Optional]),
			1E0 * (w.[Type 1] - r.[Type 1]),
			1E0 * (w.[Type 2] - r.[Type 2]),
			1E0 * (w.[Type 3] - r.[Type 3]),
			1E0 * (w.[Type 4] - r.[Type 4]),
			1E0 * (w.[Type 5] - r.[Type 5]),
			1E0 * (w.[Type 6] - r.[Type 6]),
			1E0 * (w.[Type 7] - r.[Type 7]),
			1E0 * (w.[Junk] - r.[Junk])
		)
	) c (
		[Temporal],
		[Optional],
		[Type 1],
		[Type 2],
		[Type 3],
		[Type 4],
		[Type 5],
		[Type 6],
		[Type 7],
		[Junk]    
	)
	where
		r.query in ('TIY', 'YIT', 'TOY')
)
select 
	w.tabular
from (
	select
		query,
		case query 
			when 'TIY' then '\tiy/' 
			when 'YIT' then '\yit/' 
			when 'TOY' then '\toy/' 
			when 'w/D' then '{\footnotesize Dim}'
			when 'w/F' then '{\footnotesize Fact}'
		end + ' & ' +
		isnull(format(1E0 * [Temporal], @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Optional], @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Type 1]  , @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Type 2]  , @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Type 3]  , @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Type 4]  , @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Type 5]  , @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Type 6]  , @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Type 7]  , @countFormat), '') + ' & ' +
		isnull(format(1E0 * [Junk]    , @countFormat), '') + ' \\' as tabular
	from 
		waste
	union
	select
		'Tot' as query,
		'Tot & ' +
		isnull(format(sum(1E0 * abs([Temporal])), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Optional])), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Type 1]  )), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Type 2]  )), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Type 3]  )), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Type 4]  )), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Type 5]  )), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Type 6]  )), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Type 7]  )), @countFormat), '') + ' & ' +
		isnull(format(sum(1E0 * abs([Junk]    )), @countFormat), '') + ' \\'
	from 
		waste
) w
order by
	case query when 'TIY' then 1 when 'YIT' then 2 when 'TOY' then 3 else 5 end;


