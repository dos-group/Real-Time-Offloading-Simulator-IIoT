import os
import random
import datetime as dt
from time import sleep

from mininet.net import Mininet  # type: ignore
from mininet.log import setLogLevel, info, warning  # type: ignore
from mininet.link import TCLink  # type: ignore
from mininet.clean import cleanup  # type: ignore


RUNTIME = 30
SEED = 0


def scenario(net, log_dir):
    n_clients = 40
    client_delay = '6ms'
    client_jitter = '0ms'
    client_poisson_lambda = 4
    client_payload_bytes = 100
    client_task_wcet_ms = 100
    client_task_slack_time_mean_ms = 200
    n_workers = 4
    sched_algo = 'PSWF'
    sched_delay_factor = 0.0

    print()
    print(
        '\\begin{center}\n'
        '\\begin{table}\n'
        '\\begin{tabular}{r | l}\n'
        f'Runtime & {RUNTIME} seconds\\\\\n'
        f'\\# Workers & {n_workers}\\\\\n'
        f'\\# Clients & {n_clients}\\\\\n'
        f'Client Connection Latency & Mean: {client_delay} Variance: {client_jitter}\\\\\n'
        f'Task WCET & {client_task_wcet_ms}ms\\\\\n'
        f'Laxity at Submission & {client_task_slack_time_mean_ms}ms Variance: {(client_task_slack_time_mean_ms / 3):.0f}\\\\\n'
        f'Mean Task Submission Frequency & {client_poisson_lambda} per Second\\\\\n'
        f'Task Result Payload Size & {client_payload_bytes} bytes\\\\\n'
        f'Scheduler Delay Factor & {sched_delay_factor}\\\\\n'
        '\\end{tabular}\n'
        '\\caption{\\label{tab:eval2_params} Parameters for test scenario 2}\n'
        '\\end{table}\n'
        '\\end{center}\n'
    )

    info('*** Adding switch\n')
    switch = net.addSwitch('s0')

    info('*** Adding hosts\n')
    host_count = 0
    scheduler_host = net.addHost(f'h{host_count}')
    host_count += 1
    net.addLink(scheduler_host, switch)

    worker_hosts = []
    for i in range(n_workers):
        host = net.addHost(f'h{host_count}')
        host_count += 1
        net.addLink(host, switch)
        worker_hosts.append(host)

    setLogLevel('warning')
    client_hosts = []
    for i in range(n_clients):
        host = net.addHost(f'h{host_count}')
        host_count += 1
        net.addLink(
            host,
            switch,
            cls=TCLink,
            delay=client_delay,
            jitter=client_jitter,
            bw=20,
        )
        client_hosts.append(host)

    info('*** Starting network\n')
    net.start()

    info('*** Sending commands to hosts\n')
    setLogLevel('info')

    info('*** Setting up scheduler\n')
    scheduler_ip = scheduler_host.IP()
    scheduler_port = 1337
    log_path = os.path.join(log_dir, 'scheduler.log')
    scheduler_host.log_path = log_path
    scheduler_host.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/scheduler '
        f'{scheduler_port} {sched_algo} {sched_delay_factor} '
        f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
        f'>> {log_path}'
    )
    sleep(0.2)

    info('*** Setting up workers\n')
    for i, worker_host in enumerate(worker_hosts):
        log_path = os.path.join(log_dir, f'worker{i}.log')
        worker_host.log_path = log_path
        worker_host.sendCmd(
            f'RUST_LOG=trace '
            f'../code/target/release/worker '
            f'{scheduler_ip}:{scheduler_port} '
            f'../tasks/target/task '
            f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
            f'> {log_path}'
        )
    sleep(0.2)

    info('*** Setting up clients\n')
    for i, client_host in enumerate(client_hosts):
        log_path = os.path.join(log_dir, f'client{i}.log')
        client_seed = random.randint(0, 1e8)
        client_host.log_path = log_path
        client_host.sendCmd(
            f'RUST_LOG=trace '
            f'../code/target/release/client '
            f'{scheduler_ip}:{scheduler_port} '
            f'{client_task_wcet_ms} '
            f'{client_task_slack_time_mean_ms} '
            f'{client_payload_bytes} '
            f'{client_poisson_lambda} '
            f'{client_seed} '
            f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
            f'> {log_path}'
        )
        sleep(0.05)


def main():
    if SEED > 0:
        random.seed(SEED)

    log_dir = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        f'{__file__.replace(".py", "")}_logs',
    )
    info(f'*** Saving logs to: {log_dir}\n')
    try:
        os.mkdir(log_dir)
    except FileExistsError:
        warning('*** Directory already exists.\n')
        key = input(
            '*** Press enter to overwrite all existing logs '
            'or any other key and then enter to abort\n'
        )
        if key != '':
            return

    net = Mininet()

    info('*** Adding controller\n')
    net.addController('c0')

    scenario(net, log_dir)

    info(f'*** Running for {RUNTIME} seconds\n')
    sleep(RUNTIME)
    info('*** Finished. Cleaning up\n')

    for host in reversed(net.hosts):
        host.stop()
        with open(host.log_path, 'a') as f:
            f.write(f'[{dt.datetime.now().isoformat()}Z INFO  launcher] Stopping')

    setLogLevel('warning')
    cleanup()
    setLogLevel('info')


if __name__ == '__main__':
    setLogLevel('info')
    try:
        main()
    except:
        setLogLevel('warning')
        cleanup()
        setLogLevel('info')
        raise
