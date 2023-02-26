----cast(accepted_at as date) - cast(driver_onboard_date as date) 
-----WEEKLY GRAPH
with np_ride as 
(select 
    driver_id, 
    rid.ride_id,
    ride_distance, 
    ride_duration, 
    ride_cost,
    max(case when event = 'requested_at' then "timestamp" end) requested_at, 
    max(case when event = 'accepted_at' then "timestamp" end) accepted_at, 
    max(case when event = 'arrived_at' then "timestamp" end) arrived_at, 
    max(case when event = 'picked_up_at' then "timestamp" end) picked_up_at, 
    max(case when event = 'dropped_off_at' then "timestamp" end) dropped_off_at
    from 
    (select 
       driver_id, 
       ride_id,
       ride_distance, 
       ride_duration, 
       case when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) < 5 then 5 
         when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) > 400 then 400
         else (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) end ride_cost
    from jseidman.ride_ids) as rid
    left join jseidman.ride_timestamps as ridts on rid.ride_id = ridts.ride_id
    group by 1, 2, 3, 4, 5),
value as (
select 
    np_ride.driver_id driver_id_ride,
    ride_id,
    ride_distance, 
    ride_duration, 
    ride_cost,
    requested_at, 
    accepted_at, 
    arrived_at, 
    picked_up_at, 
    dropped_off_at, 
    di.driver_id driver_id_dr,
    di.driver_onboard_date, 
    DATE_TRUNC('week', cast(accepted_at as date)) weekly,
    case when di.driver_id is not null then 'onboard_date_available' else 'unknown_onboard_date' end onboard_data_flag,
    case when cast(accepted_at as date) - cast(driver_onboard_date as date) <= 10 then 'onboarded in 10 days' 
      when cast(accepted_at as date) - cast(driver_onboard_date as date) <= 20 then 'onboarded in 10 - 20 days' 
      when cast(accepted_at as date) - cast(driver_onboard_date as date) <= 30 then 'onboarded in 20 - 30 days' 
      when cast(accepted_at as date) - cast(driver_onboard_date as date) <= 40 then 'onboarded in 30 - 40 days' 
      when cast(accepted_at as date) - cast(driver_onboard_date as date) > 40 then 'onboarded more than 41 days ago'
      else 'Unknown'
      end onboard_case
from 
    np_ride as np_ride
full outer join 
     jseidman.driver_ids as di on di.driver_id = np_ride.driver_id
    )
select 
  weekly, 
  onboard_data_flag, 
  count(distinct driver_id_ride) active_drivers, 
  count(distinct ride_id) total_rides, 
  sum(ride_cost) total_cost, 
  count(distinct concat(cast(accepted_at as date), driver_id_ride)) agg_active_days
from value
group by 1,2
;



------------------DRIVER'S TIME ON PLATFORM

----cast(accepted_at as date) - cast(driver_onboard_date as date) 
with rid as 
(select 
       driver_id, 
       ride_id,
       ride_distance, 
       ride_duration, 
       case when 2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75 < 5 then 5 
         when 2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75 > 400 then 400
         else 2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75 end ride_cost
    from jseidman.ride_ids
),
np_ride as (
select 
    di.*, 
    val.*
from 
jseidman.driver_ids as di 
left join 
    (select 
    driver_id driver_id2, 
    rid.ride_id,
    ride_distance, 
    ride_duration, 
    ride_cost,
    max(case when event = 'requested_at' then "timestamp" end) requested_at, 
    max(case when event = 'accepted_at' then "timestamp" end) accepted_at, 
    max(case when event = 'arrived_at' then "timestamp" end) arrived_at, 
    max(case when event = 'picked_up_at' then "timestamp" end) picked_up_at, 
    max(case when event = 'dropped_off_at' then "timestamp" end) dropped_off_at
    from 
    rid
    join jseidman.ride_timestamps as ridts on rid.ride_id = ridts.ride_id
    group by 1, 2, 3, 4, 5) val on di.driver_id = val.driver_id2
    ),
driver_perf as 
(select
  driver_id, 
  cast(driver_onboard_date as date) driver_onboard_date, 
  max(cast(accepted_at as date)) max_date_accepted,
  case when max(cast(accepted_at as date)) - cast(driver_onboard_date as date) is null then 0 
       else max(cast(accepted_at as date)) - cast(driver_onboard_date as date) end days_on_platform, 
  count(distinct cast(accepted_at as date)) dates_with_ride,
  count(distinct ride_id) distinct_rides, 
  sum(ride_cost) total_ride_costs
from np_ride
group by 1,2)
select 
  *
from driver_perf

------------------RETENTION 1
with np_ride as 
(select 
    driver_id, 
    rid.ride_id,
    ride_distance, 
    ride_duration, 
    ride_cost,
    max(case when event = 'requested_at' then "timestamp" end) requested_at, 
    max(case when event = 'accepted_at' then "timestamp" end) accepted_at, 
    max(case when event = 'arrived_at' then "timestamp" end) arrived_at, 
    max(case when event = 'picked_up_at' then "timestamp" end) picked_up_at, 
    max(case when event = 'dropped_off_at' then "timestamp" end) dropped_off_at
    from 
    (select 
       driver_id, 
       ride_id,
       ride_distance, 
       ride_duration, 
       case when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) < 5 then 5 
         when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) > 400 then 400
         else (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) end ride_cost
    from jseidman.ride_ids) as rid
    left join jseidman.ride_timestamps as ridts on rid.ride_id = ridts.ride_id
    group by 1, 2, 3, 4, 5),
driver_eligible_dt as 
(select 
  dates, 
  driver_onboard_date, 
  driver_id, 
  dates - cast(driver_onboard_date as date) days_since_onboard
from
(SELECT
   distinct 
   cast("timestamp" as date) dates
from jseidman.ride_timestamps
where event = 'accepted_at') as a 
left join jseidman.driver_ids as b on cast(b.driver_onboard_date as date) <= a.dates
)
select 
    a.days_since_onboard, 
    count(distinct a.driver_id) driver_eligible, 
    count(distinct b.driver_id) drivers_active
from driver_eligible_dt as a 
left join 
  (SELECT
    cast(accepted_at as date) accepted_at, 
    driver_id, 
    count(distinct ride_id) total_rides, 
    sum(ride_cost) total_cost, 
    sum(ride_distance) total_distance, 
    sum(ride_duration) total_duration
    from np_ride
    group by 1,2
  ) as b on a.driver_id = b.driver_id and a.dates = b.accepted_at
group by 1
;

--------------RETENTION 2: DEEPDIVE ---COHORT

with np_ride as (
select 
  a.*, 
  max(cast(accepted_at as date)) over (partition by a.driver_id) max_accepted_at,
  di.driver_onboard_date
from 
(select 
    driver_id, 
    rid.ride_id,
    ride_distance, 
    ride_duration, 
    ride_cost,
    ride_prime_time,
    max(case when event = 'requested_at' then "timestamp" end) requested_at, 
    max(case when event = 'accepted_at' then "timestamp" end) accepted_at, 
    max(case when event = 'arrived_at' then "timestamp" end) arrived_at, 
    max(case when event = 'picked_up_at' then "timestamp" end) picked_up_at, 
    max(case when event = 'dropped_off_at' then "timestamp" end) dropped_off_at
    from 
    (select 
       driver_id, 
       ride_id,
       ride_distance, 
       ride_duration, 
       case when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) < 5 then 5 
         when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) > 400 then 400
         else (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) end ride_cost, 
        ride_prime_time
    from jseidman.ride_ids) as rid
    left join jseidman.ride_timestamps as ridts on rid.ride_id = ridts.ride_id
    group by 1, 2, 3, 4, 5,6) as a 
join jseidman.driver_ids as di on a.driver_id = di.driver_id
),
driver_perf as 
(select
  driver_id, 
  cast(driver_onboard_date as date) driver_onboard_date, 
  max_accepted_at,
  case when max_accepted_at - cast(driver_onboard_date as date) is null then 0 
       else max_accepted_at - cast(driver_onboard_date as date) end max_min_day_diff, 
  count(distinct cast(accepted_at as date)) active_days_on_lyft,
  count(distinct ride_id) distinct_rides
from np_ride
group by 1,2,3,4)
select 
    a.driver_id,
    case when b.active_days_on_lyft <= 7 then 'less or equal to 7 active days' 
         when b.active_days_on_lyft > 7 and b.active_days_on_lyft <= 15  then 'between 7 - 15 active days'
         when b.active_days_on_lyft > 15 and b.active_days_on_lyft <= 25 then 'between 16 - 25 active days'
         when b.active_days_on_lyft > 25 and b.active_days_on_lyft <= 35 then 'between 26 - 35 active days'
         when b.active_days_on_lyft > 35 then 'more than 35 days active'
         end active_days_cohort,
    active_days_on_lyft,
    count(distinct ride_id) distinct_rides,
    sum(ride_distance)*0.0006214 ride_distance, 
    sum(ride_duration)/60 ride_time,
    sum(ride_cost) ride_costs,
    sum(ride_prime_time) sum_prime_time
from 
    np_ride as a 
    join driver_perf as b on a.driver_id = b.driver_id
    group by 1,2,3
;

----------------------RETENTION TREND


with np_ride as (
select 
  a.*, 
  max(cast(accepted_at as date)) over (partition by a.driver_id) max_accepted_at,
  dense_rank() over (partition by a.driver_id order by cast(accepted_at as date)) day_date_ranked,
  di.driver_onboard_date
from 
(select 
    driver_id, 
    rid.ride_id,
    ride_distance, 
    ride_duration, 
    ride_cost,
    ride_prime_time,
    max(case when event = 'requested_at' then "timestamp" end) requested_at, 
    max(case when event = 'accepted_at' then "timestamp" end) accepted_at, 
    max(case when event = 'arrived_at' then "timestamp" end) arrived_at, 
    max(case when event = 'picked_up_at' then "timestamp" end) picked_up_at, 
    max(case when event = 'dropped_off_at' then "timestamp" end) dropped_off_at
    from 
    (select 
       driver_id, 
       ride_id,
       ride_distance, 
       ride_duration, 
       case when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) < 5 then 5 
         when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) > 400 then 400
         else (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) end ride_cost, 
        ride_prime_time
    from jseidman.ride_ids) as rid
    left join jseidman.ride_timestamps as ridts on rid.ride_id = ridts.ride_id
    group by 1, 2, 3, 4, 5,6) as a 
join jseidman.driver_ids as di on a.driver_id = di.driver_id
),
driver_perf as 
(select
  driver_id, 
  cast(driver_onboard_date as date) driver_onboard_date, 
  max_accepted_at,
  case when max_accepted_at - cast(driver_onboard_date as date) is null then 0 
       else max_accepted_at - cast(driver_onboard_date as date) end max_min_day_diff, 
  count(distinct cast(accepted_at as date)) active_days_on_lyft,
  count(distinct ride_id) distinct_rides
from np_ride
group by 1,2,3,4)
select 
    a.driver_id,
    cast(accepted_at as date) accepted_at, 
    a.day_date_ranked,
    case when b.active_days_on_lyft <= 7 then 'less or equal to 7 active days' 
         when b.active_days_on_lyft > 7 and b.active_days_on_lyft <= 15  then 'between 7 - 15 active days'
         when b.active_days_on_lyft > 15 and b.active_days_on_lyft <= 25 then 'between 16 - 25 active days'
         when b.active_days_on_lyft > 25 and b.active_days_on_lyft <= 35 then 'between 26 - 35 active days'
         when b.active_days_on_lyft > 35 then 'more than 35 days active'
         end active_days_cohort,
    count(distinct ride_id) distinct_rides,
    sum(ride_distance)*0.0006214 ride_distance, 
    sum(ride_duration)/60 ride_time,
    sum(ride_cost) ride_costs
from 
    np_ride as a 
    join driver_perf as b on a.driver_id = b.driver_id
    group by 1,2,3,4
;


----------------BY ELIGIBLE DAYS


with np_ride as (
select 
  a.*, 
  max(cast(accepted_at as date)) over (partition by a.driver_id) max_accepted_at,
  dense_rank() over (partition by a.driver_id order by cast(accepted_at as date)) day_date_ranked,
  di.driver_onboard_date
from 
(select 
    driver_id, 
    rid.ride_id,
    ride_distance, 
    ride_duration, 
    ride_cost,
    ride_prime_time,
    max(case when event = 'requested_at' then "timestamp" end) requested_at, 
    max(case when event = 'accepted_at' then "timestamp" end) accepted_at, 
    max(case when event = 'arrived_at' then "timestamp" end) arrived_at, 
    max(case when event = 'picked_up_at' then "timestamp" end) picked_up_at, 
    max(case when event = 'dropped_off_at' then "timestamp" end) dropped_off_at
    from 
    (select 
       driver_id, 
       ride_id,
       ride_distance, 
       ride_duration, 
       case when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) < 5 then 5 
         when (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) > 400 then 400
         else (2+(1.15*ride_distance*0.0006214)+(0.22*ride_duration/60)+1.75)*(1+(ride_prime_time/100)) end ride_cost, 
        ride_prime_time
    from jseidman.ride_ids) as rid
    left join jseidman.ride_timestamps as ridts on rid.ride_id = ridts.ride_id
    group by 1, 2, 3, 4, 5,6) as a 
join jseidman.driver_ids as di on a.driver_id = di.driver_id
),
driver_eligible_dt as 
(select 
  driver_onboard_date, 
  driver_id, 
  dates - cast(driver_onboard_date as date) days_since_onboard
from
jseidman.driver_ids as a 
left join
(SELECT
   max(cast("timestamp" as date)) dates
from jseidman.ride_timestamps
where event = 'accepted_at') as b on 1 = 1
)
select 
    a.driver_id, 
    a.days_since_onboard, 
    case when a.days_since_onboard > 80 then 80
        when a.days_since_onboard > 70 then 70 
        when a.days_since_onboard > 60 then 60
        when a.days_since_onboard > 50 then 50
        when a.days_since_onboard > 40 then 40
        when a.days_since_onboard > 30 then 30
        when a.days_since_onboard > 20 then 20
        when a.days_since_onboard > 10 then 10
        when a.days_since_onboard > 0 then 0
    end days_eligible, 
    count(distinct cast(accepted_at as date)) active_days, 
    sum(ride_cost) total_revenue
from driver_eligible_dt as a 
left join np_ride as b on a.driver_id = b.driver_id
group by 1,2,3
;



