#######################################################
# load Square specific zshrc; please don't change this bit.
#######################################################
source ~/Development/config_files/square/zshrc
#######################################################

###########################################
# Feel free to make your own changes below.
###########################################

# uncomment to automatically `bundle exec` common ruby commands
# if [[ -f "$SQUARE_HOME/config_files/square/bundler-exec.sh" ]]; then
#   source $SQUARE_HOME/config_files/square/bundler-exec.sh
# fi

# load the aliases in config_files files (optional)
source ~/Development/config_files/square/aliases

[[ -f "$HOME/.aliases" ]] && source "$HOME/.aliases"
[[ -f "$HOME/.localaliases" ]] && source "$HOME/.localaliases"

# Adding pipx to path
export PATH=~/.local/bin:$PATH
export PIPX_DEFAULT_PYTHON=python3.10

# Adding libexec/bin to path
export PATH=$PATH:/opt/homebrew/opt/python@3/libexec/bin

##### notes #####
# get out of shell: deactivate
# no spaces for aliases

# zshrc
alias refresh_zshell="source ~/.zshrc"
alias zshell="open ~/.zshrc"

# Github
alias dmc="cd ~/Github/app-datamart-cco & git pull"

function gb() {
  git checkout -b "$1"
}

alias gs="git status"
alias ga="git add ."
alias gc="git commit -m primary_update"
alias gp="git push"

function acp() {
  git add .
  git commit -m "$1"
  git push
}

# squarewave syncs
alias app_datamart_cco_sync="cd ~/Github/app-datamart-cco && 
git checkout main && 
git pull && 
source .env && 
squarewave sync"

alias app_dmc_pii_sync="cd ~/Github/app-dmc-pii && 
git checkout main && 
git pull && 
source .env && 
squarewave sync"

# General
alias today="date '+%Y-%m-%d'"
