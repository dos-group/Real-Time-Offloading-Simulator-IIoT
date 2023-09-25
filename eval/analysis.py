import os
import json
import argparse
import csv
import pandas as pd  # type: ignore
import datetime as dt
from pprint import pp

pd.options.mode.chained_assignment = None  # default='warn'


def parse_scheduler(log_path) -> pd.DataFrame:
    data = []
    with open(log_path, 'r') as f:
        for line in f.readlines():
            if not 'TRACE utils] METRIC' in line:
                continue

            timestamp = line[1:28]
            split_line = line[:-1].split('METRIC ')[1].split(' ')
            params = json.loads(split_line[3])
            data.append(
                {
                    'timestamp': pd.Timestamp(timestamp),
                    'event_type': split_line[1],
                    'task_id': split_line[2],
                    'runtime_nanos': pd.to_numeric(params.get('runtime_nanos')),
                    'density': pd.to_numeric(params.get('density')),
                    'worker_id': params.get('worker_id'),
                }
            )
    return pd.DataFrame(data)


def parse_worker(log_path) -> pd.DataFrame:
    data = []
    with open(log_path, 'r') as f:
        first_line = f.readline()
        start_time = first_line[1:28]
        worker_id = first_line.split('Starting worker ')[1]
        for line in f.readlines():
            if not 'TRACE utils] METRIC' in line:
                continue

            timestamp = line[1:28]
            split_line = line[:-1].split('METRIC ')[1].split(' ')
            params = json.loads(split_line[3])
            if 'return_data_bytes' in params:
                return_data_bytes = int(params.get('return_data_bytes'))
            else:
                return_data_bytes = None
            data.append(
                {
                    'worker_id': worker_id,
                    'timestamp': pd.Timestamp(timestamp),
                    'event_type': split_line[1],
                    'task_id': split_line[2],
                    'client_addr': params.get('client_addr'),
                    'return_data_bytes': return_data_bytes,
                    'exec_time_secs': pd.to_numeric(params.get('exec_time_secs')),
                }
            )
        end_time = line[1:28]
    runtime = pd.Timestamp(end_time) - pd.Timestamp(start_time)
    return worker_id, runtime, pd.DataFrame(data)


def parse_client(log_path) -> pd.DataFrame:
    data = []
    with open(log_path, 'r') as f:
        for line in f.readlines():
            if not 'TRACE utils] METRIC' in line:
                continue

            timestamp = line[1:28]
            rest_params_line = line.split('METRIC ')[1].split('{')
            split_line = rest_params_line[0][:-1].split(' ')
            if len(rest_params_line) > 1:
                params = json.loads(f'{{{rest_params_line[1]}')
            else:
                params = {}
            if 'deadline' in params:
                deadline = pd.Timestamp(
                    dt.datetime.fromtimestamp(float(params['deadline'])), tz='UTC'
                )
            else:
                deadline = None
            if 'return_data_bytes' in params:
                return_data_bytes = int(params.get('return_data_bytes'))
            else:
                return_data_bytes = None
            data.append(
                {
                    'timestamp': pd.Timestamp(timestamp),
                    'event_type': split_line[1],
                    'task_id': split_line[2],
                    'deadline': deadline,
                    'time_to_deadline_secs': pd.to_numeric(
                        params.get('time_to_deadline_secs')
                    ),
                    'exec_time_secs': pd.to_numeric(params.get('exec_time_secs')),
                    'slack_time_secs': pd.to_numeric(params.get('slack_time_secs')),
                    'return_data_bytes': return_data_bytes,
                }
            )
    return pd.DataFrame(data)


def worker_utilization(df, runtime):
    task_count = df[df['event_type'] == 'WorkerTaskReceived']['event_type'].count()
    preemptions_df = df[df['event_type'] == 'WorkerTaskPreempted']
    preemptions_count = preemptions_df['event_type'].count()
    total_task_runtime = df[df['event_type'] == 'WorkerTaskCalculationFinished'][
        'exec_time_secs'
    ].sum()
    utilization = total_task_runtime / runtime.total_seconds()
    return task_count, utilization, preemptions_count


def all_worker_analysis(dfs, runtimes):
    data = [
        {
            'worker_id': worker_id,
            'task_count': worker_utilization(df, runtimes[worker_id])[0]
            if len(df) > 0
            else 0,
            'utilization': worker_utilization(df, runtimes[worker_id])[1]
            if len(df) > 0
            else 0.0,
            'preemption_count': worker_utilization(df, runtimes[worker_id])[2]
            if len(df) > 0
            else 0,
            'task_count': (
                df[df['event_type'] == 'WorkerTaskReceived']['event_type'].count()
                if len(df) > 0
                else 0
            ),
        }
        for worker_id, df in dfs.items()
    ]
    total_df = pd.DataFrame(data)
    task_counts = [d['task_count'] for d in data]
    utilizations = [d['utilization'] for d in data]
    preemptions = [d['preemption_count'] for d in data]
    total_mean = total_df['utilization'].mean()
    return total_mean, task_counts, utilizations, preemptions


def scheduler_analysis(df):
    print(df)
    task_count = df[df['event_type'] == 'SchedulerTaskReceived']['event_type'].count()
    preemptions_count = df[df['event_type'] == 'SchedulerTaskPreempted'][
        'event_type'
    ].count()

    schedulings_df = df[df['event_type'] == 'SchedulerTaskFinishedScheduling']
    sched_count = schedulings_df['event_type'].count()
    sched_runtime_mean = schedulings_df['runtime_nanos'].mean()
    sched_runtime_median = schedulings_df['runtime_nanos'].median()

    dispatch_time_df = df[
        df['event_type'].isin(
            [
                'SchedulerTaskReceived',
                'SchedulerTaskSentToWorker',
            ]
        )
    ]
    dispatch_time_df['time_diff'] = dispatch_time_df['timestamp'].diff()
    dispatch_time_df = (
        dispatch_time_df[dispatch_time_df['event_type'] == 'SchedulerTaskSentToWorker'][
            ['task_id', 'time_diff']
        ]
        .groupby('task_id')
        .sum()
    )
    dispatch_time_mean = dispatch_time_df["time_diff"].mean().total_seconds()
    dispatch_time_median = dispatch_time_df["time_diff"].median().total_seconds()

    worker_queue_density_df = df[df['event_type'] == 'SchedulerWorkerDensity'].groupby(
        'worker_id'
    )['density']
    worker_queue_density_mean = worker_queue_density_df.mean()
    worker_queue_density_median = worker_queue_density_df.median()

    print(f'task count:\t\t{task_count}')
    print(f'preemption count:\t{preemptions_count}')
    print(f'sched count:\t\t{sched_count}')
    print(
        f'sched runtime:\t\tmean: {sched_runtime_mean:.2f}ns\t'
        f'median: {sched_runtime_median:.2f}ns'
    )
    print(
        f'dispatch time:\t\tavg: {dispatch_time_mean:.6f}s\t'
        f'median: {dispatch_time_median:.6f}s'
    )
    print(f'worker queue density mean:\t{worker_queue_density_mean}')
    print(f'worker queue density median:\t{worker_queue_density_median}')


def client_analysis(df):
    task_created_df = df[df['event_type'] == 'ClientTaskCreated']
    df['slack_time_initial'] = (
        task_created_df['time_to_deadline_secs'] - task_created_df['exec_time_secs']
    )
    slack_time_initial_mean = df['slack_time_initial'].mean()

    df['slack_factor'] = df['time_to_deadline_secs'] / df['exec_time_secs']
    task_count = df[df['event_type'] == 'ClientTaskCreated']['event_type'].count()
    # task_created_df = df[df['event_type'] == 'ClientTaskCreated']
    # task_created_slack_factor_mean = task_created_df['slack_factor'].mean()
    # task_created_slack_factor_median = task_created_df['slack_factor'].median()

    task_accepted_count = df[df['event_type'] == 'ClientTaskAccepted'][
        'event_type'
    ].count()
    task_rejected_count = df[df['event_type'] == 'ClientTaskRejected'][
        'event_type'
    ].count()

    task_result_df = df[
        df['event_type'].isin(
            [
                'ClientTaskResultReceivedWithinDeadline',
                'ClientTaskResultReceivedAfterDeadline',
            ]
        )
    ]
    task_exec_time_mean = task_result_df['exec_time_secs'].mean()

    deadline_meet_df = df[df['event_type'] == 'ClientTaskResultReceivedWithinDeadline']
    deadline_meet_count = deadline_meet_df['event_type'].count()
    slack_time_on_completion_mean = deadline_meet_df['time_to_deadline_secs'].mean()
    deadline_meet_slack_factor_mean = deadline_meet_df['slack_factor'].mean()
    deadline_meet_slack_factor_median = deadline_meet_df['slack_factor'].median()

    deadline_miss_df = df[df['event_type'] == 'ClientTaskResultReceivedAfterDeadline']
    deadline_miss_count = deadline_miss_df['event_type'].count()
    deadline_miss_slack_factor_mean = deadline_miss_df['slack_factor'].mean()
    deadline_miss_slack_factor_median = deadline_miss_df['slack_factor'].median()

    timeout_count = df[df['event_type'] == 'ClientTaskTimeout']['event_type'].count()

    slack_factor_df = df[
        df['event_type'].isin(
            [
                'ClientTaskCreated',
                'ClientTaskResultReceivedWithinDeadline',
                'ClientTaskResultReceivedAfterDeadline',
            ]
        )
    ].sort_values(by=['task_id', 'timestamp'])
    slack_factor_df['slack_factor_diff'] = slack_factor_df['slack_factor'].diff()
    slack_factor_ratio = slack_factor_df['slack_factor'] / (
        slack_factor_df['slack_factor'] - slack_factor_df['slack_factor_diff']
    )
    df['slack_factor_ratio'] = slack_factor_ratio[
        (df['event_type'] == 'ClientTaskResultReceivedAfterDeadline')
        | (df['event_type'] == 'ClientTaskResultReceivedWithinDeadline')
    ]
    slack_factor_ratio_mean = df['slack_factor_ratio'].mean()
    slack_factor_ratio_median = df['slack_factor_ratio'].median()

    task_total_count = (
        deadline_meet_count + deadline_miss_count + timeout_count + task_rejected_count
    )
    task_accepted_count = deadline_meet_count + deadline_miss_count + timeout_count

    return (
        task_total_count,
        task_accepted_count,
        deadline_meet_count,
        task_exec_time_mean,
        slack_time_initial_mean,
        slack_time_on_completion_mean,
    )
    # print(
    #     f'Tasks accepted:\t\t{(task_accepted_count/task_total_count):.4f} '
    #     f'{task_accepted_count}/{task_total_count}'
    # )
    # print(
    #     f'Deadlines met:\t\t{(deadline_meet_count/task_accepted_count):.4f} '
    #     f'{deadline_meet_count}/{task_accepted_count}'
    # )
    # print(
    #     f'Laxity mean (seconds) on:\n'
    #     f'\tsubmission:\t{slack_time_initial_mean:.4f}\n'
    #     f'\tcompleted:\t{slack_time_on_completion_mean:.4f}'
    # )
    # print(f'task count:\t\t\t{task_count}')
    # print(f'task accepted count:\t\t{task_accepted_count}')
    # print(f'task rejected count:\t\t{task_rejected_count}')
    # print(f'deadlines meet count:\t\t{deadline_meet_count}')
    # print(f'deadline miss count:\t\t{deadline_miss_count}')
    # print(f'timeout count:\t\t\t{timeout_count}')
    # print(
    #     f'task created slack factor:\t'
    #     f'mean: {task_created_slack_factor_mean:.4f}\t'
    #     f'median: {task_created_slack_factor_median:.4f}'
    # )
    # print(
    #     f'deadline meet slack factor:\t'
    #     f'mean: {deadline_meet_slack_factor_mean:.4f}\t'
    #     f'median: {deadline_meet_slack_factor_median:.4f}'
    # )
    # print(
    #     f'deadline miss slack factor:\t'
    #     f'mean: {deadline_miss_slack_factor_mean:.4f}\t'
    #     f'median: {deadline_miss_slack_factor_median:.4f}'
    # )
    # print(
    #     f'slack factor ratio:\t\t'
    #     f'mean: {slack_factor_ratio_mean:.4f}\t'
    #     f'median: {slack_factor_ratio_median:.4f}'
    # )


def analysis(log_path):
    # Parse Logs
    worker_dfs = {}
    worker_runtimes = {}
    client_df = pd.DataFrame()
    with os.scandir(log_path) as it:
        for entry in it:
            if entry.is_file() and 'worker' in entry.name:
                worker_id, runtime, worker_df = parse_worker(entry.path)
                worker_dfs[worker_id] = worker_df
                worker_runtimes[worker_id] = runtime
            elif entry.is_file() and 'client' in entry.name:
                client_df = pd.concat(
                    [client_df, parse_client(entry.path)], ignore_index=True
                )
            else:
                scheduler_df = parse_scheduler(entry.path)

    # Scheduler
    # scheduler_analysis(scheduler_df)

    # Workers
    total_mean, task_counts, utilizations, preemptions = all_worker_analysis(
        worker_dfs, worker_runtimes
    )
    util_str = [f'{u:.2f}' for u in sorted(utilizations, reverse=True)]

    # Clients
    (
        task_total_count,
        task_accepted_count,
        deadline_meet_count,
        task_exec_time_mean,
        slack_time_initial_mean,
        slack_time_on_completion_mean,
    ) = client_analysis(client_df)

    print(f'Worker Utilization:\t' f'{total_mean:.4f} ' f'({", ".join(util_str)})')
    print(
        f'Tasks accepted:\t\t{(task_accepted_count/task_total_count):.4f} '
        f'{task_accepted_count}/{task_total_count}'
    )
    print(
        f'Deadlines met:\t\t{(deadline_meet_count/task_accepted_count):.4f} '
        f'{deadline_meet_count}/{task_accepted_count}'
    )
    print(
        f'Laxity mean (seconds) on:\n'
        f'\tsubmission:\t{slack_time_initial_mean:.4f}\n'
        f'\tcompleted:\t{slack_time_on_completion_mean:.4f}'
    )

    print()
    print(
        '\\begin{center}\n'
        '\\begin{table}\n'
        '\\begin{tabular}{r | r r r r r}\n'
        '\n& \\rot{Total Tasks}'
        '\n& \\rot{Accepted Tasks}'
        '\n& \\rot{Deadlines Met}'
        '\n& \\rot{Mean Laxity of Completed Tasks (ms)}'
        '\n& \\rot{Worker Utilization}'
        ' \\\\\n'
        '\\hline\n'
        f'& {task_total_count}'
        f' & {task_accepted_count} ({(task_accepted_count/task_total_count):.2f})'
        f' & {deadline_meet_count} ({(deadline_meet_count/task_accepted_count):.2f})'
        f' & {(slack_time_on_completion_mean * 1000):.2f}'
        f' & {total_mean:.2f} ({", ".join(util_str)})'
        ' \\\\\n'
        '\\end{tabular}\n'
        '\\caption{\\label{tab:evalX_metrics} Parameters for test scenario X}\n'
        '\\end{table}\n'
        '\\end{center}\n'
    )
    ntasks = log_path.split('_')[-2]
    uf = log_path.split('_')[-1][:-1]
    acceptance_rate = task_accepted_count/task_total_count
    hit_rate = deadline_meet_count/task_accepted_count
    util = total_mean
    sub_laxity = slack_time_initial_mean
    compl_laxity = slack_time_on_completion_mean

    result_list=[ntasks, task_total_count, uf, acceptance_rate, hit_rate, util, sub_laxity, compl_laxity, log_path.split('/')[-2]]
    with open("results.csv", mode='a') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(result_list)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('log_path', help='Path to log(s)')
    args = parser.parse_args()
    analysis(args.log_path)
