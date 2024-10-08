import re
import os
import json
import gzip
import time
import shutil
import subprocess
from subprocess import Popen, PIPE
from datetime import datetime

# *** dependência da biblioteca Paramiko;
# Doc: https://docs.paramiko.org/en/latest/
# instalar paramiko:
# $ pip install paramiko

import paramiko
from paramiko import Transport, SFTPClient

"""
>>>>> Código de automatização de backups do banco de dados do SGL.

Código de automatização de backups do banco de dados do SGL, verificação de integridade do arquivo,
e envio para N servidores definidos em uma hash(dic), passada para o construtor da classe RealizarBackupBanco.

escrito por: Ícaro martins.
"""


"""
\/ classe de operações que testa a integridade do arquivo de backup;

*** Não existe forma concreta de avaliar a integridade de um arquivo de backup do postgres,
excetuando um comando('pg_verifybackup') criado na versão 13 do PostgreSQL.
Esta classe implementa simples verificações de características de um arquivo de backup plain do postgres.
"""
class IntegridadeArquivoBackup:

    # \/  pasta onde ficará os backups do banco;
    _pasta_backups = None

    def __init__(self, arquivo, pasta_backups):
        self.arquivo = arquivo
        self._pasta_backups = pasta_backups;

    # \/ criar pasta para backup de arquivos se a mesma não existe;
    def criar_pasta_backups(self):
        # Check whether the specified path exists or not
        isExist = os.path.exists(self._pasta_backups)
        if not isExist:
            os.makedirs(self._pasta_backups)
            os.chmod(self._pasta_backups, 0o777) # << permissão completa;
    
    def mover_arquivo(self, src_path, dst_path):
        # absolute path \/;
        shutil.move(src_path, dst_path)

    # \/ tamanho do arquivo em bytes;
    def tamanho_do_arquivo(self, arquivo):
        return os.path.getsize(arquivo)

    def verificar_backup_completo(self, arquivo):
        lines_in_file = open(arquivo, 'r').readlines()
        number_of_lines = len(lines_in_file)
        # \/ uma das ultimas linhas de um aquivo de backup plain do postgres, indicando o terminio do mesmo;
        sinal_final_backup = '-- PostgreSQL database dump complete\n'
        ultimas_linhas = lines_in_file[-10:]
        if( number_of_lines > 0 and (sinal_final_backup in ultimas_linhas)): return True
        else: return False

    def verificacoes_backup(self):
        caminho_completo_arquivo = self.arquivo
        return ((self.tamanho_do_arquivo(caminho_completo_arquivo) > 0) and (self.verificar_backup_completo(caminho_completo_arquivo)))




# \/  classe para conexão ssh com o servidor remoto;
class SSHClient:

    _ssh_client = None
    _sftp = None
    _host = None

    def __init__(self, host, port, username, password):
        self.host = host
        self._host = host
        self.port = port
        self.username = username
        self.password = password
        try:
            self.create_connection(self.host, self.port, self.username, self.password)
        except:
            print('***\nErro ao conectar com o servidor '+ self.host +'.\n***')

    @classmethod
    def create_connection(cls, host, port, username, password):
        cls._ssh_client = paramiko.client.SSHClient()
        cls._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        cls._ssh_client.connect(host, username=username, password=password)
        cls._sftp = cls._ssh_client.open_sftp()
    
    def executar_comando(self, cmd):
        _stdin, _stdout,_stderr = self._ssh_client.exec_command(cmd)
        return _stdout.read().decode().strip()

    # \/ enviar arquivo para o servidor remoto;
    def upload(self, local_path, remote_path):
        flag = False
        try:
            self._sftp.put(local_path, remote_path)
            flag = True
        except IOError as e:
            print('***\nErro ao escrever arquivo no servidor '+ self._host +'.\n***')
        return flag

    def caminho_atual_servidor(self):
        return self._sftp.getcwd()
    
    # \/ método em fase de testes; 
    def mudar_diretorio(self, caminho):
        if self.file_exists(caminho):
            self._sftp.chdir(caminho)
        else: print("***\nDiretório não existe.\n***")
    
    # \/ lista de conteúdos dentro de uma pasta no servidor;
    def listar_conteudos(self, caminho):
        try:
            return self._sftp.listdir(caminho)
        except FileNotFoundError:
            return None

    def file_exists(self, remote_path):
        try:
            self._sftp.stat(remote_path)
        except IOError:
            return False
        except FileNotFoundError:
            return False
        else:
            return True

    # \/ download de arquivo do servidor remoto;
    def download(self, remote_path, local_path, retry=5):
        if self.file_exists(remote_path) or retry == 0:
            self._sftp.get(remote_path, local_path, callback=None)
        elif retry > 0:
            time.sleep(5)
            retry = retry - 1
            self.download(remote_path, local_path, retry=retry)

    def remover_arquivo(self, arquivo):
        try:
            self._sftp.remove(arquivo)
        except FileNotFoundError:
            print("***\nArquivo '"+ arquivo +"' não encontrado para remoção.\n***")
            return False
    
    # \/ criar pasta remota com permissão completa para alterações;
    def criar_pasta_remota(self, remote_path):
        flag = False
        if self.file_exists(remote_path): # Test if remote_path exists
            self._sftp.chdir(remote_path) # << mudar para o local da pasta;
            flag = True
        else:
            try:
                self._sftp.mkdir(remote_path)  # Create remote_path
                self._sftp.chdir(remote_path)
                self._sftp.chmod(remote_path, 0o777) # << permissão completa;
                flag = True
            except PermissionError: print("***\nPermissão negada ao criar pasta no servidor " + self._host + ".\n***")
            except OSError: print("***\nSocket is closed " + self._host + ".\n***")
        return flag

    def close(self):
        self._sftp.close()
        self._ssh_client.close()




# \/  classe onde será inicializada as operações de backups, e exclusão de arquivos antigos dos servidores;
class RealizarBackupBanco:

    # \/ pasta padrão onde será salva os backups do banco nos servidores;
    _PASTA_PADRAO_ARQUIVOS_BACKUP = '/home/backups_banco/'
    # \/ pasta onde será definida para salva os backups do banco nos servidores (passível de mudança);
    _pasta_backups_banco = _PASTA_PADRAO_ARQUIVOS_BACKUP
    # \/ dados do servidores onde serão enviados os backups do banco;
    _servidores_enviar = None
    # \/ nome que será utilizado para criar o arquivo de backup do banco;
    _nome_arquivo_backup = None
    _PREFIX_ARQUIVO_BACKUP = 'db_backup_'

    def __init__(self, servidores_enviar):
        self._servidores_enviar = servidores_enviar
        self._nome_arquivo_backup = self._PREFIX_ARQUIVO_BACKUP + self.data_hora_atual() + '_.gz'

    def enviar_arquivo_servidor(self, host, port, username, password, arquivo_enviar, nome_arquivo_a_ser_enviado):
        flag = False
        ssh = SSHClient(host, port, username, password)
        if ssh._sftp != None:
            ssh.criar_pasta_remota(self._pasta_backups_banco)
            flag = ssh.upload(arquivo_enviar, self._pasta_backups_banco + nome_arquivo_a_ser_enviado)
            ssh.close()
        return flag

    def realizar_backup_banco(self, usuario_banco, nome_banco_backup, nome_arquivo_backup):
        p = os.popen("pg_dump -U " + usuario_banco + " " + nome_banco_backup + " --format=p -Z 9 > " + os.getcwd() + os.path.sep + nome_arquivo_backup).read()
        return p

    def data_hora_atual(self):
        now = datetime.now()
        data_hora = str(now.day) + '-' + str(now.month) + '-' + str(now.year) + '-' + str(now.hour) + '-' + str(now.minute) + '-' + str(now.second)
        return data_hora

    def verificarIntegridadeGZIP(self, file_gzip):
        CHUNKSIZE=10000000 # 10 Mbytes
        with gzip.open(file_gzip, 'rb') as f:
            try:
                while f.read(CHUNKSIZE) != b'':
                    return True
            except:
                return False

    # \/ enviar arquivo de backup do banco para um servidor por acesso remoto ssh;
    def enviar_backup_banco_servidor(self, banco):
        saida_erro = self.realizar_backup_banco(banco['usuario'], banco['nome'], self._nome_arquivo_backup)
        if len(saida_erro) == 0:
            # \/ verificação parcial da integridade do arquivo de backup do banco;
            # test_backup_arquivo = IntegridadeArquivoBackup(self._nome_arquivo_backup, self._pasta_backups_banco)
            test_backup_arquivo = self.verificarIntegridadeGZIP(self._nome_arquivo_backup)
            if(test_backup_arquivo): # << Backup OK! ;)
                # \/ enviar para os N servidores definidos para guardar os backups;
                for servidor in self._servidores_enviar:

                    """ \/ mudar local de armazenagem de arquivo de backup no servidor,
                    onde a chave ('pasta_backup') existir no dic de informações do servidor a ser enviado o arquivo de backup do banco;
                    """
                    self.modificar_local_backup_servidor(servidor)

                    if self.teste_escrita_servidor(servidor['host'], servidor['port'], servidor['username'], servidor['password']):
                        enviado = self.enviar_arquivo_servidor(servidor['host'], servidor['port'], servidor['username'], servidor['password'], self._nome_arquivo_backup, self._nome_arquivo_backup)
                        if enviado:
                            print('backup enviado ao servidor ' + servidor['host'] + ' com sucesso.')
                    else:
                        print('Usuário não permitido para escrita no servidor ' + servidor['host'])
                # \/ remover arquivo de backup gerado em S.O. local;
                os.remove(os.getcwd() + os.path.sep + self._nome_arquivo_backup)
            else:
                print('***\nErro na verificação do arquivo de backup.\n***')
                os.remove(self._nome_arquivo_backup)
        else:
            print('***\nErro ao fazer backup.\n***')
            print(saida_erro)

    def extrair_data(self, text):
        m = re.search('(([0-9]+)-([0-9]+)-([0-9]+))-(([0-9]+)-([0-9]+)-([0-9]+))', text)
        if m:
            found = m.group(1)
            # \/ separar data e hora por espaço;
            found = m.group(1) + ' ' + m.group(5)
            return found

    # \/ retorna a lista de arquivos por ordenação de tempo de forma crescente;
    def ordernar_arquivos_por_tempos(self, arquivos):
        tempo_arquivos = {}
        tempos = []
        arquivos_ordenados = []
        for arq in arquivos:
            pri = self._PREFIX_ARQUIVO_BACKUP
            tempo = self.extrair_data(arq)
            tempo_arquivos[tempo] = arq
            tempos.append(tempo)

        tempos.sort(key=lambda x: time.mktime(time.strptime(x,"%d-%m-%Y %H-%M-%S")))
        for t in tempos: arquivos_ordenados.append(tempo_arquivos[t])
        return arquivos_ordenados

    # \/ excluir os arquivos de backups mais antigos da pasta no servidor;
    # caso o número de backups na pasta do servidor ultrapassem o número N definido,
    # será realizada uma excluisão dos restastes N arquivos mais antigos de backups;
    def excluir_arquivos_backups_antigos(self, host, port, username, password):
        # \/ número de arquivos de backups a permanecer na pasta do servidor;
        numero_arquivos_permanentes = 3
        ssh = SSHClient(host, port, username, password)
        if ssh._sftp != None:
            arquivos_pasta = ssh.listar_conteudos(self._pasta_backups_banco)
            if arquivos_pasta != None and len(arquivos_pasta) > numero_arquivos_permanentes:
                # \/ obter as exceções dos ultimos 3 arquivos de backups salvos na pasta do servidor;
                arquivos_excluir = self.ordernar_arquivos_por_tempos(arquivos_pasta)[:-numero_arquivos_permanentes]
                # \/ excluir os arquivos de backups mais antigos da pasta, deixando apenas os N ultimos backups mais recentes inseridos;
                for arq in arquivos_excluir: ssh.remover_arquivo(self._pasta_backups_banco + arq)
            ssh.close()
    
    # \/ excluir os arquivos de backups mais antigos das pastas de cada servidor onde é enviado o backup;
    def excluir_backups_antigos_servidores(self):
        for servidor in self._servidores_enviar:
            self.excluir_arquivos_backups_antigos(servidor['host'], servidor['port'], servidor['username'], servidor['password'])

    # \/ realizar um teste de escrita no servidor onde será salvo um backup do banco;
    def teste_escrita_servidor(self, host, port, username, password):
        flag = False
        ssh = SSHClient(host, port, username, password)
        if ssh._sftp != None:
            flag = ssh.criar_pasta_remota(self._pasta_backups_banco)
            ssh.close()
        return flag

    """ \/ mudar local de armazenagem de arquivo de backup no servidor,
    onde a chave ('pasta_backup') existir no dic de informações do servidor a ser enviado o arquivo de backup do banco;
    """
    def modificar_local_backup_servidor(self, servidor):
        # \/ modificar local se a pasta foi definida no dic do servidor;
        if 'pasta_backup' in servidor:
            self._pasta_backups_banco = servidor['pasta_backup']
        else:
            # \/ retorna o local para salvar no local padrão definido na classe;
            self._pasta_backups_banco = self._PASTA_PADRAO_ARQUIVOS_BACKUP

    # \/ remover arquivos encontrados em um determinado local;
    def remover_arquivos_regex(self, caminho):
        arquivos = os.listdir(caminho)
        regex_arquivo_encontrar = "("+ self._PREFIX_ARQUIVO_BACKUP +"([0-9]{1,2}\-[0-9]{1,2}\-[0-9]{4}\-[0-9]{1,2}\-[0-9]{1,2}\-[0-9]{1,2})_(\.gz|\.sql))"
        for arq in arquivos:
            if re.match(regex_arquivo_encontrar, arq) != None:
                caminho_remover = caminho + os.path.sep + arq
                if os.path.exists(caminho_remover):
                    try:
                        os.remove(caminho_remover)
                    except PermissionError: print("***\nPermissão negada ao remover arquivo.\n***")

    def testes_servidores(self):
        for servidor in self._servidores_enviar:

            """ \/ mudar local de armazenagem de arquivo de backup no servidor,
            onde a chave ('pasta_backup') existir no dic de informações do servidor a ser enviado o arquivo de backup do banco;
            """
            self.modificar_local_backup_servidor(servidor)

            if self.teste_escrita_servidor(servidor['host'], servidor['port'], servidor['username'], servidor['password']):
                print( servidor['host'] + ' ok.')
            else:
                print('Usuário não permitido para escrita no servidor ' + servidor['host'])
    
    # \/ inicializar procedimentos de backup;
    def inicializar_procedimentos_backups(self, banco):
        # \/ inicializar procedimentos de backups e envio aos servidores;
        self.enviar_backup_banco_servidor(banco)

        # \/ remover arquivo de backup que ficam armazenados na pasta /root;
        self.remover_arquivos_regex('/root')

        # \/ inicializar procedimentos de exclusão de backups antigos dos servidores;
        self.excluir_backups_antigos_servidores()



if __name__ == '__main__':

    # *** É de extrema importância que os usuários dos servidores possuam permissão
    # para salvar arquivos no sistema operacional;

    # \/ ler arquivo JSON de configuração;
    arquivo_config = 'servidores-enviar.json'
    if os.path.exists(arquivo_config):
        file = open(arquivo_config)
        # returns JSON object as a dictionary
        data = json.load(file)
        if 'servidores_enviar' in data and 'bancos' in data:
            # \/ array de dics dos servidores a serem enviados os backups;
            servidores_enviar = data['servidores_enviar']
            # \/ dados do bancos a fazer o backup;
            bancos = data['bancos']
            for banco in bancos:
                # \/ inicializar procedimentos de backups e envio aos servidores;
                backups = RealizarBackupBanco(servidores_enviar)
                backups.inicializar_procedimentos_backups(banco)
    else: print('***\nErro, nenhum arquivo \''+ arquivo_config +'\' no diretório encontrado.\n***')

    # \/ realizar teste de escrita nos servidores;
    # backups.testes_servidores()

    # \/ teste de escrita no servidor;
    # servidor = servidores_enviar[1]
    # backups.teste_escrita_servidor(servidor['host'], servidor['port'], servidor['username'], servidor['password'])
