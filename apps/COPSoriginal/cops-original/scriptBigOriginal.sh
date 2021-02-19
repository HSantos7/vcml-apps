#!/bin/sh

COPS_DIR=/home/santos/masterthesis/apps/COPSoriginal/cops-original/

COMPILE="mvn clean install ; tmux wait-for -S compile-finished"
SERVER00='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_0_0"'
SERVER01='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_0_1"'
SERVER02='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_0_2"'
SERVER10='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_1_0"'
SERVER11='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_1_1"'
SERVER12='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_1_2"'
SERVER20='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_2_0"'
SERVER21='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_2_1"'
SERVER22='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/server_configs/server_2_2"'
CLIENT0='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/ycsb_client_Client_0"'
CLIENT1='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/ycsb_client_Client_1"'
CLIENT2='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPS/cops/tools/generated_files/cops_big/ycsb_client_Client_2"'

tmux new-session -s cops -d

#split screen in have horizontally
tmux split-window -v -t 0
tmux split-window -v -t 0
tmux split-window -v -t 0

#split the windows in half vertically
tmux split-window -h -t 0
tmux split-window -h -t 0
tmux split-window -h -t 3
tmux split-window -h -t 3
tmux split-window -h -t 6
tmux split-window -h -t 6
tmux split-window -h -t 9
tmux split-window -h -t 9

for _pane in $(tmux list-panes -a -F '#{pane_id}'); do \
  tmux send-keys -t ${_pane} "cd $COPS_DIR" ENTER; done

tmux send-keys -t 0 "$COMPILE" ENTER
for _Num in 1 2 3 4 5 6 7 8 ; do \
  tmux send-keys -t ${_Num} "cd cops-server-original" ENTER; done


tmux send-keys -t 9 "cd cops-client-original" ENTER
tmux send-keys -t 10 "cd cops-client-original" ENTER
tmux send-keys -t 11 "cd cops-client-original" ENTER
tmux wait-for compile-finished
tmux send-keys -t 0 "cd cops-server-original" ENTER


tmux send-keys -t 0 "$SERVER00" ENTER
tmux send-keys -t 1 "$SERVER10" ENTER
tmux send-keys -t 2 "$SERVER20" ENTER
tmux send-keys -t 3 "$SERVER01" ENTER
tmux send-keys -t 4 "$SERVER11" ENTER
tmux send-keys -t 5 "$SERVER21" ENTER
tmux send-keys -t 6 "$SERVER02" ENTER
tmux send-keys -t 7 "$SERVER12" ENTER
tmux send-keys -t 8 "$SERVER22" ENTER

sleep 5

tmux send-keys -t 11 "$CLIENT2" ENTER
tmux send-keys -t 10 "$CLIENT1" ENTER
tmux send-keys -t 9 "$CLIENT0" ENTER

sleep 5

tmux send-keys -t 9 ENTER
tmux send-keys -t 10 ENTER
tmux send-keys -t 11 ENTER

tmux attach -t cops