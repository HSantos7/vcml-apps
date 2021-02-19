#!/bin/sh

COPS_DIR=/home/santos/masterthesis/apps/COPSoriginal/cops-original

COMPILE="mvn clean install ; tmux wait-for -S compile-finished"
SERVER0='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPSoriginal/cops-original/tools/generated_files/cops_cor/server_configs/server_0_0"'
SERVER1='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPSoriginal/cops-original/tools/generated_files/cops_cor/server_configs/server_1_0"'
SERVER2='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPSoriginal/cops-original/tools/generated_files/cops_cor/server_configs/server_2_0"'
CLIENT0='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPSoriginal/cops-original/tools/generated_files/cops_cor/ycsb_client_Client_0"'
CLIENT1='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPSoriginal/cops-original/tools/generated_files/cops_cor/ycsb_client_Client_1"'
CLIENT2='mvn exec:java -Dexec.args="/home/santos/masterthesis/apps/COPSoriginal/cops-original/tools/generated_files/cops_cor/ycsb_client_Client_2"'

tmux new-session -s cops -d

#split screen in have horizontally
tmux split-window -v

#split the windows in half vertically
tmux split-window -h -t 0
tmux split-window -h -t 0
tmux split-window -h -t 3
tmux split-window -h -t 3

for _pane in $(tmux list-panes -a -F '#{pane_id}'); do \
  tmux send-keys -t ${_pane} "cd $COPS_DIR" ENTER; done
  
tmux send-keys -t 0 "$COMPILE" ENTER
tmux send-keys -t 1 "cd cops-server-original" ENTER
tmux send-keys -t 2 "cd cops-server-original" ENTER
tmux send-keys -t 3 "cd cops-client-original" ENTER
tmux send-keys -t 4 "cd cops-client-original" ENTER
tmux send-keys -t 5 "cd cops-client-original" ENTER
tmux wait-for compile-finished
tmux send-keys -t 0 "cd cops-server-original" ENTER


tmux send-keys -t 0 "$SERVER0" ENTER
tmux send-keys -t 1 "$SERVER1" ENTER
tmux send-keys -t 2 "$SERVER2" ENTER

sleep 3

tmux send-keys -t 3 "$CLIENT0" ENTER
tmux send-keys -t 4 "$CLIENT1" ENTER
tmux send-keys -t 5 "$CLIENT2" ENTER

sleep 4

tmux send-keys -t 3 ENTER
tmux send-keys -t 4 ENTER
tmux send-keys -t 5 ENTER

tmux attach -t cops