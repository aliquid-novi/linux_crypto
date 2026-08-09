# linux_crypto

This project is based on a crypto delta-neutral trading strategy being run autonomously with Linux to simulate tasks done by risk managers/quantitative traders. 

## The Strategy

delta neutral -> long 1 unit of spot, short 1 unit of perp of the same cryptocurrency. Goal is to harvest the funding rate, and entry is on if the expected return on entering this trade is positive. this is calculated by:

$$
\begin{aligned}
\text{Entry Basis} &= P_{\text{perp,bid}} - P_{\text{spot,ask}} \\[4pt]

\text{Expected Basis Return}
&= \frac{\text{Entry Basis}}{P_{\text{spot,ask}}} \\[4pt]

\text{Expected Funding Return}
&= f_{\text{predicted}} \times H \\[4pt]

\text{Round-Trip Fees}
&= 2\left(F_{\text{spot}} + F_{\text{perp}}\right) \\[4pt]

\text{Expected Return}
&= \text{Expected Basis Return}
+ \text{Expected Funding Return}
- \text{Round-Trip Fees}
\end{aligned}
$$


included in the strategy file is a 'heartbeat' that writes to a JSON file that writes the timestamp, time since last spot and futures fetched, and whether or not a position is open. 

## The supporting linux system

the supporting linux system makes sure that the strategy is running, with use of Linux's `systemd`. 

shell files that systemd touches to make sure the strategy is running are the `check_strategy.sh` and `start_strategy.sh`. 

The `start_strategy.sh` shell script creates a log dir and runtime dir
- The log dir includes the strategy’s stdout and stderr. this allows us to see what errors/outputs occured after the terminal closes.
- the runtime directory includes a `strategy.pid` and `heartbeat.json` file. The `strategy.pid` is created each time the start_strategy.sh is run to ensure a unique ID is run parallel to the **current** run. the `heartbeat.json` file is created by the `delta_neut_strat.py` file, containing the aforementioned 'heartbeat' details. 

To integrate all these files into one coherent system, usage of 'systemd' is implemented. `systemd` is a service manager for linux distributions that initialises user space, can manage system services and controls parallel start up, among other things. in this case, to put simply, it begins the `start_strategy.sh` script and if it crashes for any reason, will restart the `start_strategy.sh` script, ensuring that the strategy works continously. 
