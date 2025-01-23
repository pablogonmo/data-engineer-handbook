-- On call run book for Pablo Inc Growth Pipeline --

# Pipelines

1. Profit = subscription costs paid - expenses on all accounts
    - Unit level profit: profit / # of subscribers (+ info about cost per account)

2. Growth
    - Increase in number of accounts per month, increase in size of accounts that are about to renew (increase in $ increased frrom upgrade in account)

3. Engagement
    - How many users using technology in company
    - How many hours per day are all users on account spending time on account

4. Aggregate pipeline to Executives/CFO (ultimately presented to investors)
    - Weekly

5. Aggregate pipeline to Experiment team
    - Data science team uses unit level/daily level data to conduct experiments on AB testing features being rolled out to different accounts
    - Weekly preferred, monthly - to drive direction of product team

The pipelines that will affect investor's are the Profit, Growth, Engagement and Aggregate Pipeline to Investors - so the next few pages will be runbooks for each pipeline

# Runbooks

## Profit Pipeline
- Types of data:
    - Revenue from accounts
    - What is spent on assets, other services according to Ops team
    - Aggregated salaries by team
- Owners: Finance Team/Risk Team
- Secondary Owner: Data Engineering
- Common Issues:
    - Numbers don't align with numbers on accounts/filings - these numbers need to be verified by an accountant if so
- SLA's:
    - Numbers will be reviewed once a month by account team
- Oncall schedule:
    - Monitored by BI in profit team, and folks rotate watching pipeline on weekly basis

## Growth Pipeline
- Types of data: Changes made to the account type,
    - Num. of users with license increased
    - Account stopped subscribing
    - Account continued subscription for the next calendar year
- Owners: Accounts Team
- Secondary Owner: Data Engineer Taem
- Common Issues:
    - Time series dataset - so the current status of an account is missing since AE team forgot to 
    - A clue that it's missing is a previous step that is required in is missing (ex: only changes A, C, when step B is required to change to C)
- SLA's:
    - Data will contain latest account statuses by end of week
- Oncall schedule:
    - No on call if pipeline fails, but pipeline will be debug by team during working hours

## Pipeline Name: Engagement
- Owners: Software Frontend Team
- Secondary Owner: Data Engineer Team
- Engagement metrics come from clicks from all users using platforms in different teams
    - Sometimes data associated with click will arrive to kafka queue extremely late - much after the data has already been aggregated for a downstream pipeline
    - If kafka goes down, all user clicks from website will not be sent to kafka, therefore not sent to the downstream metrics
    - Sometimes the same event will come through the pipeline multiple times - data must be de-duplicated
- SLA's:
    - Data will arrive within 48hrs - if latest timestamp > the current timestamp - 48 hrs, then the SLA is not met
    - Issues will be fixed within 1 week
- Oncall schedule:
    - One person on DE team owns pipeline each week - there is a contact on SWE team for questions
    - Next week - 30 min meeting to transfer onboarding to the next person

## Pipeline Name: Aggregated data for executives and investors
- Owners: Business Analytics team
- Secondary Owner: Data Engineer team
- Common Issues:
    - Spark joins to join accounts to revenue, and engagement may fail - a lot of data is involved in the joins and there may be OOM issues
    - Issues with stale data with previous pipelines - queue backfills periodically
    - Missing data may cause issues with NA or divide by 0 errors
- SLA's:
    - Issues will be fixed by end of month, when reports are given to executives and investors
- Oncall schedule:
    - Around last week of month, DE's are monitoring that pipelines of the data from the month are running smoothly