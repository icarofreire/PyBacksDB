# --- $$$ ---
# script para realizar backup's automaticos do banco de dados e
# fazer push de commits para o repositório remoto.
#
# https://gitlab.com/devses/backups-db.git
# -------------------

import os
import shutil
import subprocess
from subprocess import Popen, PIPE
import os.path
from os import path
from datetime import datetime

# informações do repositório;
url_repositorio = "https://gitlab.com/devses/backups-db.git"

def clone(url_repositorio):
    p = subprocess.run(["git", "clone", url_repositorio], capture_output=True, text=True)

def fetch_all():
    p = subprocess.run(["git", "fetch", "--all"], capture_output=True, text=True)

def pull():
    p = subprocess.run(["git", "pull", "origin", "master"], capture_output=True, text=True)

def push():
    p = subprocess.run(["git", "push", "origin", "master"], capture_output=True, text=True)
    if p.stdout.find('done.') != -1:
        return True
    else:
        return False

def add_all():
    p = subprocess.run(["git", "add", "."], capture_output=True, text=True)

def commit():
    now = datetime.now()
    data_hora = str(now.day) + '/' + str(now.month) + '/' + str(now.year) + '-' + str(now.hour) + ':' + str(now.minute) + ':' + str(now.second)
    p = subprocess.run(["git", "commit", "-m", "'backup SGL - " + data_hora+ "'"], capture_output=True, text=True)

# EX: url_repositorio = "https://gitlab.com/devses/backups-db.git"
def se_repositorio_existe(url_repositorio):
    p = subprocess.run(["git", "ls-remote", url_repositorio], capture_output=True, text=True)
    saida_stderr = p.stderr # saida de erro stderr;
    nao_encontrado = saida_stderr.find("not found")
    if len(saida_stderr) > 0 and nao_encontrado != -1:
        return False
    else:
        return True

def se_existem_distancias_entre_branches():
    p = subprocess.run(["git", "rev-list", "master", "..", "remotes/origin/master"], capture_output=True, text=True)
    saida_stdout = p.stdout
    if len(saida_stdout) > 0:
        return True
    else:
        return False

# ***

if se_repositorio_existe(url_repositorio):
    fetch_all()
    pull()
    if se_existem_distancias_entre_branches():
        print("Existe(m) diferença(s) entre branch local a remota.")
        add_all()
        commit()
        if push():
            print("Push realizado.")
        else:
            print("Erro ao realizar push.")
    else:
        print("Repositório local atualizado.")
else:
    clone(url_repositorio)
