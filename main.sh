#!/bin/bash

##调度系统启动脚本
nohup hadoop jar mr2es-1.0.0-SNAPSHOT-jar-with-dependencies.jar >> mr2es.log 2>&1 &
