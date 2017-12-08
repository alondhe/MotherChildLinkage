select distinct 
       candidate_moms.person_id as mom_person_id,
	     candidate_moms.year_of_birth as mom_yob,
	     candidate_babies.person_id as baby_person_id,
	     candidate_babies.date_of_birth as baby_dob,
	     ROW_NUMBER() OVER (PARTITION BY candidate_babies.person_id order by candidate_babies.person_id, candidate_moms.person_id) as moms_per_kid
into #candidates
from
(
	select p1.person_id, 
		     ppp1.family_source_value, 
		     pp1.year_of_birth
	from @resultsDatabaseSchema.pregnancy_episodes p1 --pregnancy episode cohort location
	inner join @pppDatabaseSchema.@pppTableName ppp1 
		on ppp1.person_id = p1.person_id
		and p1.episode_end_date >= ppp1.payer_plan_period_start_date 
		and p1.episode_end_date <= ppp1.payer_plan_period_end_date
	inner join @cdmDatabaseSchema.person pp1 
		on pp1.person_id = p1.person_id
	where p1.outcome = 'LB/DELIV' --livebirth pregnancy episode outcome
	group by p1.person_id, ppp1.family_source_value, pp1.year_of_birth
) candidate_moms
inner join 
(
	select p1.person_id, 
	       ppp1.family_source_value,
		     p1.year_of_birth,
		     min(op1.observation_period_start_date) as observation_period_start_date,
		     min(datefromparts(p1.year_of_birth, 
			   case when p1.month_of_birth is not null and p1.month_of_birth >= 1 and p1.month_of_birth <= 12 then p1.month_of_birth else month(op1.observation_period_start_date) end, 
			   case when p1.day_of_birth is not null and p1.day_of_birth >= 1 and p1.day_of_birth <= 31 then p1.day_of_birth else 1 end)) as date_of_birth
	from @pppDatabaseSchema.@pppTableName ppp1
	inner join @cdmDatabaseSchema.person p1
		on ppp1.person_id = p1.person_id
	inner join @cdmDatabaseSchema.observation_period op1
		on p1.person_id = op1.person_id
		and op1.observation_period_start_date >= ppp1.payer_plan_period_start_date
		and op1.observation_period_start_date <= ppp1.payer_plan_period_end_date
	where year(observation_period_start_date) - p1.year_of_birth = 0
	and p1.person_id not in (
		select person_id from @resultsDatabaseSchema.pregnancy_episodes --pregnancy episode cohort location
    )
	group by p1.person_id, ppp1.family_source_value, p1.year_of_birth
			 
) candidate_babies
	on candidate_moms.family_source_value = candidate_babies.family_source_value
inner join @cdmDatabaseSchema.observation_period op1
	on candidate_moms.person_id = op1.person_id
	and candidate_babies.date_of_birth >= op1.observation_period_start_date 
	and candidate_babies.date_of_birth <= op1.observation_period_end_date
where candidate_moms.person_id <> candidate_babies.person_id
;

delete from #candidates 
where baby_person_id in (select baby_person_id from #candidates where moms_per_kid >= 2)
;

select cmc1.mom_person_id,
       cmc1.mom_yob,
       cmc1.baby_person_id, 
	   cmc1.baby_dob as date_of_birth_from_op,
	   moms_births.episode_end_date as date_of_birth_from_alg
into #probables
from #candidates cmc1
inner join
(
	select person_id, episode_end_date
	from @resultsDatabaseSchema.pregnancy_episodes
	where outcome = 'LB/DELIV' 
) moms_births
	on cmc1.mom_person_id = moms_births.person_id
	and moms_births.episode_end_date >= dateadd(dd, -60, cmc1.baby_dob) 
	and moms_births.episode_end_date <= dateadd(dd, 60, cmc1.baby_dob)  
;

insert into @cdmDatabaseSchema.fact_relationship 
  (domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
select distinct
  A.domain_concept_id_1,
  A.fact_id_1,
  A.domain_concept_id_2,
  A.fact_id_2,
  A.relationship_concept_id
from
(
  select
    56 as domain_concept_id_1, 
    mom_person_id as fact_id_1, 
    56 as domain_concept_id_2, 
    baby_person_id as fact_id_2, 
    @motherRelationshipId as relationship_concept_id
  from #probables

  union
  
  select 
    56 as domain_concept_id_1, 
    baby_person_id as fact_id_1, 
    56 as domain_concept_id_2, 
    mom_person_id as fact_id_2, 
    @childRelationshipId as relationship_concept_id
  from #probables
) A;

TRUNCATE TABLE #candidates;
DROP TABLE #candidates;

TRUNCATE TABLE #probables;
DROP TABLE #probables;