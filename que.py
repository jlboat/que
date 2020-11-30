#!/usr/bin/env python3

from collections import OrderedDict
from datetime import datetime
import subprocess as sp
import argparse
import json
import sys
import re

def parse_arguments():
    """Parse arguments passed to script"""
    parser = argparse.ArgumentParser(description=
            "Check the set of jobs on the scheduler queue and \
                    \npresent the data in a clean, readable format. \
                    \n**Requires either '--user' or '--queue' be specified. \
                    \nUsage: {0} [-u user -q queue] \n".format(sys.argv[0]),
                    formatter_class = argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-u", "--user", type=str, required=False,
            help="view specific user data", action="store")
    parser.add_argument("-q", "--queue", type=str, required=False,
            help="view queue with substring 'QUEUE'", action="store")
    parser.add_argument("-s", "--state", type=str, required=False,
            help="view jobs in specific state",
            action="store")
    parser.add_argument("-n", "--name", type=str, required=False,
            help="view jobs with substring 'NAME'",
            action="store")
    return parser


def get_qstat_json():
    """Call qstat for JSON data"""
    qstat_output = sp.check_output(['qstat','-f','-Fjson'])
    clean_qstat_output = qstat_output.replace(
            b'"Job_Name":inf,',b'"Job_Name":"Unknown",') #.replace(b'\\', b'\\\\')
    try:
        results = json.loads(clean_qstat_output.decode("utf-8","ignore").replace('^"^^',''),
                      object_pairs_hook=OrderedDict)
    except json.decoder.JSONDecodeError as err:
        sys.stderr.write("{0}\n".format(err))
        sys.stderr.write("Error reading queue. See que.error.log\n")
        with open("que.error.log", 'w') as f:
            for line in clean_qstat_output.decode("utf-8","ignore"):
                f.write(line)
        sys.exit(1)
    return results
   

def summarize_json(filtered_json):
    """Summarize running jobs"""
    # Get total number on queue
    number_jobs = len(filtered_json)
    # Get number per queue and R/Q/Other
    queue_job_counts = {}
    job_state_counts = {'R':0,'Q':0,'Other':0}
    for key, value in json_data.items():
        try:
            queue_job_counts[value['queue']] = queue_job_counts[value['queue']] + 1
        except KeyError:
            queue_job_counts[value['queue']] = 1
        try:
            job_state_counts[value['job_state'].rstrip()] = job_state_counts[value['job_state']] + 1
        except KeyError:
            job_state_counts['Other'] = job_state_counts['Other'] + 1
    summary = (f"NumberOfJobs:{number_jobs}  JobsPerQueue:{queue_job_counts}  " + 
              f"JobStates:{job_state_counts}")
    return summary


def convert_walltime(used_walltime, total_walltime):
    try:
        used_walltime = used_walltime['resources_used']["walltime"]
    except KeyError:
        used_walltime = "00:00:00"
    total_walltime = total_walltime["walltime"]
    used_walltime = used_walltime.split(':')[0:2]
    total_walltime = total_walltime.split(':')[0:2]
    percentage_numerator = (int(used_walltime[0])*60)+int(used_walltime[1])
    percentage_denominator = (int(total_walltime[0])*60)+int(total_walltime[1])
    percentage = 100 * (percentage_numerator / float(percentage_denominator))
    walltime = (f"{':'.join(used_walltime)}" + '/' + 
                f"{':'.join(total_walltime)} " + 
                f"({percentage:2.0f}%)")
    return walltime


def convert_cpu_efficiency(used_cpus, total_cpus):
    try:
        used_cpus = used_cpus['resources_used']["cpupercent"]
    except KeyError:
        used_cpus = 0
    total_cpus = total_cpus["ncpus"]
    return f"{(used_cpus / (total_cpus*100))*100:3.0f}%"


def convert_mem_efficiency(used_mem, total_mem):
    try:
        used_mem = int(used_mem['resources_used']["mem"].replace("kb","").replace("b",""))
    except KeyError:
        used_mem = 0
    total_mem = total_mem["mem"]
    units = total_mem[len(total_mem)-2:]
    total_mem = float(total_mem[0:len(total_mem)-2])
    if units.lower() == "gb":
        total_mem = total_mem * 1024 * 1024
    elif units.lower() == "mb":
        total_mem = total_mem * 1024
    elif units.lower() == "kb":
        total_mem = total_mem
    elif units.lower() == "0b":
        return f"0%"
    return f"{(used_mem / total_mem)*100:3.0f}%"


def fill_none(variable):
    if variable is None:
        return ""
    else:
        return variable


def filter_json(json_data, user, queue, state, name):
    """Given arguments, filter JSON jobs"""
    filtered_json = OrderedDict()
    # [user, queue, state, name, mem, ncpus, jobid]
    spacing = {"user": max(7, len(user)), 
            "queue":max(7, len(queue)),
            "state":5,
            "name":7,
            "mem":3,
            "ncpus":4,
            "jobid":8,
            "walltime":8,
            "cpu_efficiency":len("CPU Eff."),
            "mem_efficiency":len("MEM Eff.")}
    for jobid, job in json_data["Jobs"].items():
        if type(job["Job_Name"]) == int:
                job["Job_Name"] = str(job["Job_Name"])
        try:
            if ((user in job["Job_Owner"]) and 
                    (queue in job["queue"]) and 
                    (state.upper() in job["job_state"]) and
                    (name in job["Job_Name"])):
                filtered_json[jobid] = job
                spacing["name"] = max(spacing["name"], len(job["Job_Name"]))
                spacing["mem"] = max(spacing["mem"], len(job["Resource_List"]["mem"]))
                spacing["queue"] = max(spacing["queue"], len(job["queue"]))
                spacing["walltime"] = max(spacing["walltime"], 
                                          len(convert_walltime(job,  
                                                               job["Resource_List"]
                                                               )
                                             )
                                          )
                spacing["ncpus"] = max(spacing["ncpus"], 
                        len(str(job["Resource_List"]["ncpus"])))
                spacing["cpu_efficiency"] = max(spacing["cpu_efficiency"],
                        len(convert_cpu_efficiency(job, 
                            job["Resource_List"])))
                spacing["mem_efficiency"] = max(spacing["mem_efficiency"],
                        len(convert_mem_efficiency(job,
                            job["Resource_List"])))
        except TypeError:
            print(job)
            sys.exit(1)
    return filtered_json, spacing


def generate_table(json_data, spacing):
    """Take filtered JSON and put into readable table"""
    for key, value in spacing.items():
        spacing[key] = value + 1
    csv_table = (f"\033[1;32;40m{'JobID':^{spacing['jobid']}}" +
                 f"{'JobName':^{spacing['name']}}" +
                 f"{'Mem':^{spacing['mem']}}" + 
                 f"{'CPUs':^{spacing['ncpus']}}" + 
                 f"{'User':^{spacing['user']}}" + 
                 f"{'Queue':^{spacing['queue']}}" + 
                 f"{'State':^{spacing['state']}}" + 
                 f"{'Walltime':^{spacing['walltime']}}" +
                 f"{'%CPU':^{spacing['cpu_efficiency']}}" + 
                 f"{'%MEM':^{spacing['mem_efficiency']}}\033[00m\n")
    even = 0
    for key, value in json_data.items():
        walltime = convert_walltime(value, 
                                    value['Resource_List'])
        cpu_efficiency = convert_cpu_efficiency(value, 
                                                value["Resource_List"])
        mem_efficiency = convert_mem_efficiency(value, value["Resource_List"])
        job = (f"{key.replace('.pbs02',''):^{spacing['jobid']}}" + 
               f"{value['Job_Name']:^{spacing['name']}}" +
               f"{value['Resource_List']['mem']:^{spacing['mem']}}" +
               f"{value['Resource_List']['ncpus']:^{spacing['ncpus']}}" +
               f"{value['Job_Owner'].split('@')[0]:^{spacing['user']}}" +
               f"{value['queue']:^{spacing['queue']}}" +
               f"{value['job_state']:^{spacing['state']}}" + 
               f"{walltime:^{spacing['walltime']}}" + 
               f"{cpu_efficiency:^{spacing['cpu_efficiency']}}" + 
               f"{mem_efficiency:^{spacing['mem_efficiency']}}\033[00m\n")
        if even % 2 == 0:
            csv_table = csv_table + "\033[1;37;48m" + job
        else:
            csv_table = csv_table + "\033[0;37;48m" + job
        even += 1
    return csv_table


if __name__ == "__main__":
    parser = parse_arguments()
    args = parser.parse_args()
    if not (args.user or args.queue):
        parser.error("que -h")
    json_data = get_qstat_json()
    json_data, spacing = filter_json(json_data, 
            fill_none(args.user), 
            fill_none(args.queue), 
            fill_none(args.state),
            fill_none(args.name))
    json_summary = summarize_json(json_data)
    print("\033[1;31;48m{0}  {1}\033[00m".format(datetime.now(), json_summary))
    data_table = generate_table(json_data, spacing)
    print(data_table)
    if len(json_data) > 30:
        print("\033[1;31;48m{0}  {1}\033[00m".format(datetime.now(), json_summary))
