import os
import json
import argparse
import pandas as pd  # type: ignore
import datetime as dt
import matplotlib.pyplot as plt
from pprint import pp

pd.options.mode.chained_assignment = None  # default='warn'


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
                    'time_to_deadline_ms': pd.to_numeric(
                        params.get('time_to_deadline_secs')
                    )
                    * 1000,
                    'initial_slack_time_ms': pd.to_numeric(
                        params.get('initial_slack_time_secs')
                    )
                    * 1000,
                    'exec_time_secs': pd.to_numeric(params.get('exec_time_secs')),
                    'slack_time_secs': pd.to_numeric(params.get('slack_time_secs')),
                    'return_data_bytes': return_data_bytes,
                }
            )
    return pd.DataFrame(data)


def analysis(log_path):
    # Parse Logs
    df = pd.DataFrame()
    with os.scandir(log_path) as it:
        for entry in it:
            if entry.is_file() and 'client' in entry.name:
                df = pd.concat([df, parse_client(entry.path)], ignore_index=True)

    task_result_df = df[
        df['event_type'].isin(
            [
                'ClientTaskResultReceivedWithinDeadline',
                'ClientTaskResultReceivedAfterDeadline',
            ]
        )
        & (df['initial_slack_time_ms'] >= df['time_to_deadline_ms'])
    ]

    plot = task_result_df.plot.scatter(
        x='initial_slack_time_ms',
        y='time_to_deadline_ms',
        xlabel='Initial Laxity (ms)',
        ylabel='Laxity when Task Result was received by Client (ms)',
        s=3,
    )
    plt.axhline(y=0.0, color='r', linestyle='-', label='Deadline')
    plt.legend()
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('log_path', help='Path to log(s)')
    args = parser.parse_args()
    analysis(args.log_path)
