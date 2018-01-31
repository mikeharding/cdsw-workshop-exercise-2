#!/bin/bash

hadoop fs -mkdir /tmp/BasketballStats
hadoop fs -put data/* /tmp/BasketballStats
