#!/bin/bash

hadoop fs -mkdir /tmp/BasketballStats
hadoop fs -put basketball-stats/data/* /tmp/BasketballStats
