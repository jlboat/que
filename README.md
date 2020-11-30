# que
A python script for checking a PBS Pro Job Scheduler queue

```bash
> python que.py -h
```

usage: que.py [-h] [-u USER] [-q QUEUE] [-s STATE] [-n NAME]

Check the set of jobs on the scheduler queue and                     
present the data in a clean, readable format.                     

\*\*Requires either '--user' or '--queue' be specified.                     
Usage: que.py [-u user -q queue] 

optional arguments:
  -h, --help            show this help message and exit
  -u USER, --user USER  view specific user data
  -q QUEUE, --queue QUEUE
                        view specific queue
  -s STATE, --state STATE
                        view jobs in specific state
  -n NAME, --name NAME  view jobs with substring 'name'

```bash
> python que.py -q c1
```

![Image of output](https://github.com/jlboat/que/images/Screenshot_2020-11-30_094241.jpg)
