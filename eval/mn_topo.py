import random
import inspect
from time import sleep
from os.path import join

from mininet.net import Mininet  # type: ignore
from mininet.cli import CLI  # type: ignore
from mininet.log import setLogLevel, info  # type: ignore
from mininet.link import TCLink  # type: ignore
from mininet.clean import cleanup  # type: ignore
from mn_wifi.cli import CLI  # type: ignore


LOG_DIR = './logs/'

# RUNTIME = 10
RUNTIME = 30
# RUNTIME = 300

SEED = 0
# SEED = 1337
# SEED = 1338


def topo3(net):
    n_workers = 4
    sched_delay_factor = 2.5
    sched_algo = 'PSWF'
    n_clients = 10
    client_delay = '6ms'
    client_jitter = '2ms'
    client_poisson_lambda = 1
    client_payload_bytes = 100
    client_task_wcet_ms = 300
    client_task_slack_time_mean_ms = 200

    fname = inspect.stack()[0][3]

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
    scheduler_host.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/scheduler '
        f'{scheduler_port} {sched_algo} {sched_delay_factor} '
        f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
        f'> {join(LOG_DIR, f"scheduler_{fname}.log")}'
    )
    sleep(0.2)

    info('*** Setting up workers\n')
    for i, worker_host in enumerate(worker_hosts):
        log_path = join(LOG_DIR, f'worker{i}_{fname}.log')
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
        log_path = join(LOG_DIR, f'client{i}_{fname}.log')
        client_seed = random.randint(0, 1e8)
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


def topo2(net):
    n_workers = 4
    n_fast_clients = 50
    n_slow_clients = 0
    host_count = 0
    # sched_algo = 'PSFF'
    # sched_algo = 'PSBF'
    sched_algo = 'PSWF'
    sched_delay_factor = 1.0

    fname = inspect.stack()[0][3]

    info('*** Add switches\n')
    switch = net.addSwitch('s0')
    # net.addNAT().configDefault()

    info('*** Add hosts\n')
    scheduler_host = net.addHost(f'h{host_count}')
    host_count += 1
    net.addLink(scheduler_host, switch, cls=TCLink, delay='0.5ms', bw=1000)

    worker_hosts = []
    for i in range(n_workers):
        host = net.addHost(f'h{host_count}')
        host_count += 1
        net.addLink(host, switch, cls=TCLink, delay='0.5ms', bw=1000)
        worker_hosts.append(host)

    fast_client_hosts = []
    for i in range(n_fast_clients):
        host = net.addHost(f'h{host_count}')
        host_count += 1
        net.addLink(host, switch, cls=TCLink, delay='20ms', jitter='0ms', bw=20)
        fast_client_hosts.append(host)

    slow_client_hosts = []
    for i in range(n_slow_clients):
        host = net.addHost(f'h{host_count}')
        host_count += 1
        net.addLink(host, switch, cls=TCLink, delay='20ms', jitter='10ms', bw=20)
        slow_client_hosts.append(host)

    info('*** Starting network\n')
    net.start()

    info('*** Sending commands to hosts\n')

    info('Setting up scheduler\n')
    scheduler_ip = scheduler_host.IP()
    scheduler_port = 1337
    scheduler_host.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/scheduler '
        f'{scheduler_port} {sched_algo} {sched_delay_factor} '
        f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
        f'> {join(LOG_DIR, f"scheduler_{fname}.log")}'
    )
    sleep(1.0)

    info('Setting up workers\n')
    for i, worker_host in enumerate(worker_hosts):
        log_path = join(LOG_DIR, f'worker{i}_{fname}.log')
        worker_host.sendCmd(
            f'RUST_LOG=trace '
            f'../code/target/release/worker '
            f'{scheduler_ip}:{scheduler_port} '
            f'../tasks/target/task '
            f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
            f'> {log_path}'
        )
    sleep(1.0)

    info('Setting up fast clients\n')
    for i, client_host in enumerate(fast_client_hosts):
        log_path = join(LOG_DIR, f'client{i}_{fname}.log')
        client_host.sendCmd(
            f'RUST_LOG=trace '
            f'../code/target/release/client '
            f'{scheduler_ip}:{scheduler_port} '
            f'150 100 10000 0.5 {random.randint(0, 1e8)} '
            f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
            f'> {log_path}'
        )
        sleep(0.05)

    info('Setting up slow clients\n')
    for i, client_host in enumerate(slow_client_hosts, start=n_fast_clients):
        log_path = join(LOG_DIR, f'client{i}_{fname}.log')
        client_host.sendCmd(
            f'RUST_LOG=trace '
            f'../code/target/release/client '
            f'{scheduler_ip}:{scheduler_port} '
            f'5500 2000 10000 0.1 {random.randint(0, 1e8)} '
            f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
            f'> {log_path}'
        )
        sleep(0.1)


def topo1(net):
    info('*** Add switches\n')
    s1 = net.addSwitch('s1')

    info('*** Add hosts\n')
    h1 = net.addHost('h1')
    h2 = net.addHost('h2')
    h3 = net.addHost('h3')

    info('*** Add links\n')
    net.addLink(h1, s1, cls=TCLink, delay='25ms', bw=200)
    net.addLink(h2, s1, cls=TCLink, delay='25ms', bw=200)
    net.addLink(h3, s1, cls=TCLink, delay='25ms', bw=200)

    info('*** Starting network\n')
    net.start()

    # info(f'H1 IP: {h1.IP()}\n')
    # info(f'H2 IP: {h2.IP()}\n')
    # info(f'H3 IP: {h3.IP()}\n')

    info('*** Sending commands to hosts\n')

    info('Setting up scheduler\n')
    scheduler_ip = h1.IP()
    scheduler_port = 1337
    h1.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/scheduler '
        f'{scheduler_port} PSBF '
        f'&> {join(LOG_DIR, "scheduler_topo1.log")}'
    )
    sleep(1.0)

    info('Setting up workers\n')
    h2.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/worker '
        f'{scheduler_ip}:{scheduler_port} '
        f'../tasks/target/task '
        f'&> {join(LOG_DIR, "worker_topo1.log")}'
    )
    sleep(1.0)

    info('Running client\n')
    h3.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/client '
        f'{scheduler_ip}:{scheduler_port} '
        f'1000 500 500 1 3000 {random.randint(0, 1e8)} '
        f'&> {join(LOG_DIR, "client_topo1.log")}'
    )


def delay_topo(net):
    info('*** Add switches\n')
    s1 = net.addSwitch('s1')

    info('*** Add hosts\n')
    h1 = net.addHost('h1')
    h2 = net.addHost('h2')
    h3 = net.addHost('h3')

    info('*** Add links\n')
    net.addLink(h1, s1, cls=TCLink, delay='0ms')
    net.addLink(h2, s1, cls=TCLink, delay='0ms')
    net.addLink(h3, s1, cls=TCLink, delay='50ms', jitter='20ms')

    info('*** Starting network\n')
    net.start()

    info('Setting up scheduler\n')
    scheduler_ip = h1.IP()
    scheduler_port = 1337
    sched_algo = 'PSWF'
    h1.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/scheduler '
        f'{scheduler_port} {sched_algo} '
        f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
        f'> {join(LOG_DIR, f"delay_scheduler.log")}'
    )
    sleep(1.0)

    info('Setting up workers\n')
    h2.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/worker '
        f'{scheduler_ip}:{scheduler_port} '
        f'../tasks/target/task '
        f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
        f'> {join(LOG_DIR, "delay_worker.log")}'
    )
    sleep(1.0)

    info('Running client\n')
    h3.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/client '
        f'{scheduler_ip}:{scheduler_port} '
        f'450 200 1 0.5 {random.randint(0, 1e8)} '
        f'2>&1 >/dev/null | grep --line-buffered -v "mio::poll" '
        f'> {join(LOG_DIR, "delay_client.log")}'
    )


def test_topo(net):
    info('*** Add switches\n')
    s1 = net.addSwitch('s1')
    s2 = net.addSwitch('s2')

    info('*** Add hosts\n')
    h0 = net.addHost('h0')
    h1 = net.addHost('h1')
    h2 = net.addHost('h2')
    h3 = net.addHost('h3')

    info('*** Add links\n')
    net.addLink(s1, s2)
    net.addLink(h0, s1)
    net.addLink(h1, s1)
    net.addLink(h2, s2)
    net.addLink(h3, s2)
    # net.addLink(h1, s1, cls=TCLink, bw=200, delay='10ms', jitter='5ms')
    # net.addLink(h2, s1, cls=TCLink, bw=200, delay='10ms')
    # net.addLink(h3, s1, cls=TCLink, bw=200, delay='10ms')
    # setLogLevel('debug')
    # for i in range(56):
    #     host = net.addHost(f'h{i}')

    for i in range(50):
        net.addLink(h1, s1)

    for i in range(50):
        net.addLink(h2, s2)

    setLogLevel('debug')
    net.addLink(net.hosts[-1], s1)
    setLogLevel('info')

    # setLogLevel('info')
    info('*** Starting network\n')
    net.start()

    CLI(net)
    net.stop()


def run_topo(runtime, topo):
    if SEED > 0:
        random.seed(SEED)
    net = Mininet()

    info('*** Adding controller\n')
    net.addController('c0')

    topo(net)

    info(f'*** Running for {runtime} seconds\n')
    sleep(runtime)

    # CLI(net)
    # net.stop()
    info('*** Finished. Cleaning up\n')

    for host in reversed(net.hosts):
        host.stop()

    setLogLevel('warning')
    cleanup()
    setLogLevel('info')


if __name__ == '__main__':
    setLogLevel('info')
    try:
        # run_topo(0, test_topo)
        # run_topo(RUNTIME, topo2)
        run_topo(RUNTIME, topo3)
        # run_topo(60, delay_topo)
        # run_topo(0, test_topo)
        # wifi_topo()
    except:
        setLogLevel('warning')
        cleanup()
        setLogLevel('info')
        raise
