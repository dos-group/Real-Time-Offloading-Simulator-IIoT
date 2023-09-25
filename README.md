# Offloading and Scheduling of Real-Time Tasks on Edge Resources in IIoT Environments

To build and run this project `make` and the Rust toolchain are required.
Detailed installation instructions can be found at https://www.rust-lang.org/tools/install

## Building the Project

Clone the repository.

Navigate into the source code directory:

```bash
cd ./code/
```

Build the project:

```bash
cargo build --workspace
cargo build -r
```

Build the dummy task:
```bash
cd ../tasks/
make
cd ../code/
```


## Running Manually

Running the scheduler, worker and clients:

### Scheduler
The scheduler needs to be started first.  
From the `./code/` directory navigate to the scheduler directory:

```bash
cd scheduler
```

Starting the scheduler:
```bash
RUST_LOG=<log_level> cargo run <port> <allocation_strategy> <delay_factor>
```
The `log_level` can be one of `trace, debug, info, warn, error, fatal`.  
The `port` can be chosen freely.  
The `allocation_strategy` can be one of `GS, PSFF, PSBF, PSWF`. `PSWF` is the recommended option (stands for partitioned schedule with worst fit allocation).  
The `delay_factor` can be a decimal number greater or equal to 0.0. Recommended is a value between 1.0 and 5.0.


### Worker
Open another terminal.
Repeat the following steps for as many workers as you would like to start.

From the `./code/` directory navigate to the worker directory:

```bash
cd worker
```

Starting the worker:
```bash
RUST_LOG=<log_level> cargo run <scheduler_ip:port> ../../tasks/target/task
```
The `log_level` can be one of `trace, debug, info, warn, error, fatal`. The events for the evaluation metrics are logged as `trace` events.  
The `scheduler_ip:port` needs to be in the format `123.123.123.123:12345`.


### Client
Open another terminal.
Repeat the following steps for as many clients as you would like to start.

From the `./code/` directory navigate to the client directory:

```bash
cd client
```

Starting the client:
```bash
RUST_LOG=<log_level> cargo run <scheduler_ip:port> <execution_time> <laxity> <return_data_bytes> <interval_lambda> <rng_seed>
```
The `log_level` can be one of `trace, debug, info, warn, error, fatal`. The events for the evaluation metrics are logged as `trace` events.  
The `scheduler_ip:port` needs to be in the format `123.123.123.123:12345`.  

For the task parameters please refer to the thesis paper. An example would be:
```bash
RUST_LOG=trace cargo run 192.168.178.78:1337 1000 100 1 0.5 0
```

The client will now start to submit tasks to the scheduler.

Everything is logged to `stderr` by default.




## Running the Evaluation Scenarios

The evaluation scenarios need Mininet to be installed.
`Sudo` rights might be necessary.
Detailed instructions can be found at https://mininet.org/download/.  
The analysis dependencies are installed with Poetry (https://python-poetry.org/docs/).

From the root of the repository navigate to the eval directory:
```bash
cd eval
```

Install the required Python dependencies:
```bash
poetry install
```

Run one of the scenarios:
```bash
sudo python test_scenarios/scenario3b.py
```
Depending on the Mininet installation, `sudo` might not be necessary.

For details about the parameters please refer to the respective section in the paper and the scenario scripts.

All logs for an executed scenario are put into the respective `test_scenarios/scenario<...>_logs/` directory.

### Analysis

Two analysis scripts are available for analysing the output logs of the scenarios.
Pass the respective log directory as parameter:
```bash
poetry run python analysis.py test_scenarios/scenario<...>_logs/
poetry run python scatter.py test_scenarios/scenario<...>_logs/
```




