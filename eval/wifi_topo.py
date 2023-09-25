from time import sleep
from os.path import join

from mininet.node import Controller  # type: ignore
from mininet.log import info  # type: ignore
from mn_wifi.net import Mininet_wifi  # type: ignore
from mn_wifi.cli import CLI  # type: ignore


def wifi_topo():
    net = Mininet_wifi(controller=Controller)

    info("*** Creating nodes\n")
    ap1 = net.addAccessPoint(
        'ap1',
        ssid='new-ssid',
        mode='g',
        channel='1',
        failMode="standalone",
        mac='00:00:00:00:00:01',
        position='50,50,0',
    )

    sta1 = net.addStation(
        'sta1', mac='00:00:00:00:00:02', ip='10.0.0.1/8', position='30,60,0'
    )
    sta2 = net.addStation(
        'sta2', mac='00:00:00:00:00:03', ip='10.0.0.2/8', position='70,30,0'
    )

    s1 = net.addSwitch('s1')

    h1 = net.addHost('h1', ip='10.0.0.3/8')
    h2 = net.addHost('h2', ip='10.0.0.4/8')

    c1 = net.addController('c1')

    info("*** Configuring propagation model\n")
    net.setPropagationModel(model="logDistance", exp=4.5)

    info("*** Configuring wifi nodes\n")
    net.configureWifiNodes()

    info("*** Creating links\n")
    net.addLink(ap1, s1)
    net.addLink(h1, s1)
    net.addLink(h2, s1)

    info("*** Starting network\n")
    net.build()
    c1.start()
    ap1.start([c1])
    s1.start([c1])

    # info("*** Running CLI\n")
    CLI(net)

    # info("*** Stopping network\n")
    net.stop()

    return

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
    sta1.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/client '
        f'{scheduler_ip}:{scheduler_port} '
        f'1000 500 500 1 3000 '
        f'&> {join(LOG_DIR, "client1_topo1.log")}'
    )
    sta2.sendCmd(
        f'RUST_LOG=trace '
        f'../code/target/release/client '
        f'{scheduler_ip}:{scheduler_port} '
        f'1000 500 500 1 3000 '
        f'&> {join(LOG_DIR, "client2_topo1.log")}'
    )

    sleep(RUNTIME)
