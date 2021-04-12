#!/usr/bin/env bash

export projhome="/ccs/techint/proj/spider2-snapshot-analysis/"
export projprefix="$projhome/sw/prefix"
export projpfshome="/gpfs/alpine/proj-shared/stf008/hs2/spider2-snapshot-analysis"

export JAVA_HOME="$projprefix/jdk1.8.0_241"


# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/sw/andes/python/3.7/anaconda-base/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/sw/andes/python/3.7/anaconda-base/etc/profile.d/conda.sh" ]; then
        . "/sw/andes/python/3.7/anaconda-base/etc/profile.d/conda.sh"
    else
        export PATH="/sw/andes/python/3.7/anaconda-base/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

module load wget
module load python/3.7-anaconda3

conda activate spider2-snapshot

