# linux_crypto

This project takes a simple crypto delta-neutral paper trading strategy and builds a Linux-based operational layer around it.

The main goal of the project is not to demonstrate a sophisticated trading edge, but to get hands-on experience with some of the tasks involved in running and monitoring a continuously operating trading algorithm: process management, logging, health checks, stale-data detection and service supervision.

## The Strategy

The strategy is a simple delta-neutral spot/perpetual trade:

- Long BTC spot
- Short the equivalent quantity of the BTC perpetual
- Attempt to capture positive perpetual funding while remaining approximately neutral to movements in the underlying BTC price

The strategy receives spot and perpetual market data through Kraken WebSocket feeds.

A position is opened when the estimated return from the basis and predicted funding is greater than the estimated round-trip transaction costs.

The entry calculation is:

```math
\begin{aligned}
\text{Entry Basis}
&= P_{\text{perp,bid}} - P_{\text{spot,ask}} \\

\text{Expected Basis Return}
&= \frac{\text{Entry Basis}}{P_{\text{spot,ask}}} \\

\text{Expected Funding Return}
&= f_{\text{predicted}} \cdot H \\

\text{Round-Trip Fees}
&= 2\left(F_{\text{spot}} + F_{\text{perp}}\right) \\

\text{Expected Return}
&= \text{Expected Basis Return}
+ \text{Expected Funding Return}
- \text{Round-Trip Fees}
\end{aligned}
```

This is intentionally a simplified paper strategy. The focus of the project is the operational infrastructure around the strategy rather than proving that the strategy itself has an edge.

## Heartbeat and Health Monitoring

`delta_neut_strat.py` also runs a heartbeat alongside the trading logic.

Every few seconds it writes a `heartbeat.json` file containing:

- the current timestamp
- time since the latest spot market update
- time since the latest futures market update
- whether a paper position is currently open

This is useful because simply knowing that a Python process exists does not necessarily mean that the strategy is healthy. For example, the process could still be running while one of the WebSocket feeds has stopped receiving data.

`health_check.py` reads this information and can therefore distinguish between basic process health and application/data-feed health.

## Linux Operations Layer

Before using `systemd`, I built several shell scripts to understand the basic Linux process lifecycle manually.

### `start_strategy.sh`

The startup script:

- creates the required `logs/` and `runtime/` directories
- launches the strategy using the project's virtual-environment Python interpreter
- runs the strategy in the background
- redirects stdout and stderr to `logs/strategy.log`
- captures the PID assigned to the Python process and writes it to `runtime/strategy.pid`
- prevents accidentally starting another copy when the recorded process is already running

The PID file provides a simple way for the other operational scripts to identify the process associated with that particular strategy run.

### `check_strategy.sh`

This performs a basic process-level check by reading the stored PID and checking whether that process still exists.

This was useful for understanding the difference between:

1. a Python file existing on disk
2. that file being executed as a Linux process
3. the operating system assigning that process a PID
4. monitoring whether that process is still alive

### `stop_strategy.sh`

The stop script reads the stored PID, sends a termination signal to that process and removes the stale PID file afterwards.

## Logs and Runtime State

The project separates persistent logs from temporary runtime information:

```text
linux_crypto/
├── delta_neut_strat.py
├── health_check.py
├── start_strategy.sh
├── stop_strategy.sh
├── check_strategy.sh
├── logs/
│   └── strategy.log
└── runtime/
    ├── heartbeat.json
    └── strategy.pid
```
`logs/strategy.log` stores the strategy's stdout and stderr, allowing errors and normal output to be inspected even when the original terminal session is no longer open. Additionally, `runtime/`'s directory stores temporary state used to monitor the currently running system. 

## systemd 

After implementing the strategy and shell scripts manually, I moved the strategy to `systemd` - this acts as a supervisor for the strategy, directly lauching the strategy using the project's virtual-environment Python interpreter. The `check_strategy.sh`, `start_strategy.sh` and `stop_strategy.sh` served as manual checks to understand the manual process-management steps that `systemd` now does. The service was configured with:

``Restart=on-failure``
``RestartSec=5``

This configuration ensures that if the Python strategy exits unexpectedly, `systemd` will wait 5 seconds before starting a new instance. 

## Next Steps

Now that this simple system ensures that the operational health of the algorithm is secured via `systemd` and the strategy data is monitored by the strategy script itself by outputting `heartbeat.json` on fixed intervals, the next step would be to connect either paper or live trading to a more sophisticated trading strategy as the current infrustructure around the strategy is operationally robust in making sure the strategy is running. 
