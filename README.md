# linux_crypto

This project is based on a crypto delta-neutral trading strategy being run autonomously with Linux to simulate tasks done by risk managers/quantitative traders. 

## The Strategy

delta neutral -> long 1 unit of spot, short 1 unit of perp of the same cryptocurrency. Goal is to harvest the funding rate. 

## The supporting linux system

the supporting linux system makes sure that the strategy is running, with use of Linux's `systemd`. 

shell files that systemd touches to make sure the strategy is running are the `check_strategy.sh` and `start_strategy.py`. 

python file health_check.py does [] 


## overall system

s
