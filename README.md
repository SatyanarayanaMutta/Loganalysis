# Project - 1: LogsAnalysis 
## Developed by Naga Satyanarayana Mutta

LogsAnalysis,part of the udacity [fullstack web developer nanodegree](https://www.udacity.com/course/full-stack-web-developer-nanodegree--nd004)

## Project Overview

This is single file project this generates the reports from the news article database.This reporting tool is a python program using the ``psycopg2`` module to connect to the ``PostgreSQL`` database.

## Requirements

1) Python         (Programming Language)
2) vagrant        (A virtual Environment)
3) virtual Box    (An Open Source Virtualization Product)
4) Git Bash or power shell (for windows)
5) psycopg2       (library)
6) postgreSQL     (database)


## Project Contents

1) app_log.py --> is the main file to run this logAnalysis Project executes the SQLQuaries.

2) README.md --> Step By Step Procedure to run this LogAnalysis project.

3) newsdata.zip --> The Newsdata ZipFile contains populate news.

4) Result.txt --> Executed expected output my Project


## Project Setup

1) Install Vagrant [https://www.vagrantup.com/]

2) Install VirtualBox[https://www.virtualbox.org/wiki/Download_Old_Builds_5_1] to install and run your projects on virtual machine.

3) Vagrant Setup file to this Download or Clone fullstack-nanodegree-vm[https://github.com/udacity/fullstack-nanodegree-vm] repository.

4) Launch the vagrant VM inside vagrant sub-directory in the downloaded fullstack-nanodegree-vm repository using commands:

Step 1 => ``$vagrant up``

Step 2 =>  ``$vagrant ssh`` 

Step 3 => Change Directory to vagrant =>  ``$cd /vagrant/``

Step 4 => Change Directory to project folder => $cd mylogsanlysis

Step 5 => Download the [newsdata](https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip). Unzip the file in order to extract newsdata.sql. This file should be inside the `vagrant/mylogsanlysis/`.

Step 6 => Load news data in database => ``$psql -d news -f newsdata.sql ``

Step 7 => Connect news data to database =>  ``$psql -d news``

Step 8 => Create helping view for third problem create view using 

'''select * from (select x.days_count ,
round(cast((100*y.hits) as numeric) / cast(x.hits as numeric) , 2)
as error_per from (select date(time) as days_count , count(*) as hits
from log group by days_count) as x inner
join (select date(time) as days_count, count(*) as hits from log
where status like '%404%' group by days_count) as y
on x.days_count = y.days_count) as t where error_per > 1.0;'''

Step 9 => Type `\q` command for exit from PostgreSQL.

Step 10 => Run Python file => ``$python3 app_log.py``

## Helpful Resources
=> [PEP 8 -- Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/)
=> [PostgreSQL 9.5 Documentation](https://www.postgresql.org/docs/9.5/static/index.html)
=> [Vagrant](https://www.vagrantup.com/downloads)
=> [VirtualBox](https://www.virtualbox.org/wiki/Downloads)