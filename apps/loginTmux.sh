#!/bin/sh

COPS_DIR=COPS/

COMPILE="mvn clean install ; tmux wait-for -S compile-finished"

tmux new-session -s cops-login -d

#split screen in have horizontally
tmux split-window -v
tmux split-window -v
tmux split-window -v

#split the windows in half vertically
tmux split-window -h -t 0
tmux split-window -h -t 0
tmux split-window -h -t 3
tmux split-window -h -t 3
tmux split-window -h -t 6
tmux split-window -h -t 6
tmux split-window -h -t 9
tmux split-window -h -t 9

tmux select-layout tiled


tmux send-keys -t 0 "ssh inode20" ENTER
tmux send-keys -t 1 "ssh inode22" ENTER
tmux send-keys -t 2 "ssh inode23" ENTER
tmux send-keys -t 3 "ssh inode24" ENTER
tmux send-keys -t 4 "ssh inode25" ENTER
tmux send-keys -t 5 "ssh inode26" ENTER
tmux send-keys -t 6 "ssh inode27" ENTER
tmux send-keys -t 7 "ssh inode28" ENTER
tmux send-keys -t 8 "ssh inode29" ENTER
tmux send-keys -t 9 "ssh inode09" ENTER
tmux send-keys -t 10 "ssh inode11" ENTER
tmux send-keys -t 11 "ssh inode02" ENTER



for _pane in $(tmux list-panes -a -F '#{pane_id}'); do \
  tmux send-keys -t ${_pane} "cd $COPS_DIR" ENTER; done


  

#tmux send-keys -t 0 "$SERVER0" ENTER
#tmux send-keys -t 1 "$SERVER1" ENTER
#tmux send-keys -t 2 "$SERVER2" ENTER

#sleep 3

#tmux send-keys -t 5 "$CLIENT2" ENTER
#tmux send-keys -t 3 "$CLIENT0" ENTER
#tmux send-keys -t 4 "$CLIENT1" ENTER

#sleep 4

#tmux send-keys -t 3 ENTER
#tmux send-keys -t 4 ENTER
#tmux send-keys -t 5 ENTER

tmux attach -t cops-login