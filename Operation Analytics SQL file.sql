create database Operation_analytics;

use Operation_analytics;

/*CASE STUDY 1*/

/*Uploading the dataset*/

CREATE TABLE job_data (ds DATE,job_id INT NOT NULL, actor_id INT NOT NULL, event VARCHAR(50) NOT NULL,     
language VARCHAR(15) NOT NULL,  time_spent INT NOT NULL, org CHAR(2) ); 

INSERT INTO job_data (ds, job_id, actor_id, event, language, time_spent, org) 
VALUES ('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'), 
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005,'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

/*Query 1,Number of jobs reviewed*/
/*Calculate the number of jobs reviewed per hour per day for November 2020?*/


select ds as day,round(count(job_id)/sum(time_spent)*3600) as jobs_reviewed_perhour
from job_data
where ds between '2020-11-01' AND '2020-11-30'
group by ds;

/*Query 2,Throughput:The no. of events happening per second.*/
/*Let’s say the above metric is called throughput. Calculate 7 day rolling average of throughput? 
For throughput, do you prefer daily metric or 7-day rolling and why?*/

select ds, event_or_events_per_day, 
round(avg(event_or_events_per_day) over(order by ds rows between 6 preceding and current row),2) as 7_day_rolling_avg 
from (select ds, count(distinct event) as event_or_events_per_day 
from job_data
group by ds) as temptable;

/*Please explain your throghput preference i  ppt*/

/*Query 3,Percentage share of each language: Share of each language for different contents*/
/* Calculate the percentage share of each language in the last 30 days?*/

select language as languages,concat(round(count(*)*100/(select count(*)
from job_data),2),'%') as percetage_share
from job_data
group by language;

/*Query 4,Duplicate rows: Rows that have the same value present in them*/
/*Let’s say you see some duplicate rows in the data. How will you display duplicates from the table?*/

select *
from job_data;

select ds, COUNT(ds) as no_of_duplicate
from operation_analytics.job_data
group by ds
having no_of_duplicate > 1;

/*CASE STUDY 2*/

/*Uploading the dataset*/


Create TABLE events (user_id INT NOT NULL,occurred_at DATE, event_type VARCHAR(50) NOT NULL,event_name VARCHAR(50),
location VARCHAR(50) NOT NULL, device VARCHAR(15) NOT NULL, user_type INT NOT NULL); 


/*Query 1,User Engagement:To measure the activeness of a user.Measuring if the user finds quality in a product/service.
/*Calculate the weekly user engagement?*/

select week(occurred_at) as week,count(distinct user_id) as weekly_user_engagement
from events
where event_type='engagement'
group by week(occurred_at)
order by week(occurred_at);

/*Query 2,User Growth: Amount of users growing over time for a product.
/*Calculate the user growth for product?*/

select year,week_num, new_user_activated,
new_user_activated-lag(new_user_activated) over( order by year,week_num ) as user_growth
from(select year(activated_at) as year,week(activated_at) as week_num,count(user_id) as new_user_activated 
from users 
where activated_at is not null and state='active'
group by year,week_num
order by year,week_num) as temptable;


/*Query 3,Weekly Retention: Users getting retained weekly after signing-up for a product*/
/*Calculate the weekly retention of users-sign up cohort?*/


select t1.week_num,(t2.old_users - t1.new_users)as Retained_users
from(select week(occurred_at) as week_num,
count(distinct user_id) as new_users
from events
where event_type = "signup_flow"
group by week_num) as t1
Join
(select week(occurred_at) as week_num,
count(distinct user_id) as old_users
from events
where event_type = "engagement"
group by week_num) as t2
on t1.week_num = t2.week_num;

/*Query 4,Weekly Engagement:To measure the activeness of a user. 
Measuring if the user finds quality in a product/service weekly*/
/*Your task: Calculate the weekly engagement per device?*/

select week(occurred_at) as weeks,device,count(distinct user_id) as device_engagement
from events
group by device, week(occurred_at)
order by week(occurred_at);

/*Query 5,Email Engagement: Users engaging with the email service.
/*Calculate the email engagement metrics?*/

select distinct week(occurred_at) as week_num,
count(distinct case when action = 'sent_weekly_digest' then user_id end) as email_digest,
count(distinct case when action ='email_open' then user_id end) as email_open,
count(distinct case when action = 'email_clickthrough' THEN user_id end) as click_throgh,
count(distinct case when action='sent_reengagement_email' then user_id end) as reengagement_emails
from email_events
group by week(occurred_at);

/*--THE END--*/