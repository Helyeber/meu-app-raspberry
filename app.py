#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SERVIDOR PYTHON - INTEGRACAO LOVABLE CLOUD COM SQL SERVER
VERSAO OTIMIZADA PARA RASPBERRY PI LINUX
"""
import json
import time
import threading
import sys
import os
from datetime import datetime
import pymssql
import requests

# ==================================================
# CONFIGURACOES DO SISTEMA
# ==================================================
FIREBASE_URL = "https://alphacred-3a710-default-rtdb.firebaseio.com/LOVABLE/consulta2"

# Configuracoes das tabelas
TABELA_TITULO = "FINANCEIROTITULO"
TABELA_PARCELA = "FINANCEIROTITULOPARCELA"
COLUNA_EMPRESA_ID = "pessoa_id"
COLUNA_VALOR_TOTAL = "financeirotituloparcela_valortotal"
COLUNA_SITUACAO_ID = "financeirosituacao_id"

RECONNECT_MAX_ATTEMPTS = 24
RECONNECT_DELAY = 5

# Configuracoes SQL Server
SQLSERVER_CONFIG = {
    "server": "172.16.1.32",
    "port": 1433,
    "user": "ti",
    "password": "m@ster#2023#",
    "timeout": 10,
    "login_timeout": 10
}

BANCO_SQLSERVER = "SYM2"

# ==================================================
# SISTEMA DE LOGS SIMPLIFICADO
# ==================================================
class SistemaLogs:
    def __init__(self, arquivo_log="lovable_servidor.log"):
        self.arquivo_log = arquivo_log
        
    def log(self, mensagem, nivel="INFO"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        linha = f"{timestamp} | {nivel} | {mensagem}"
        
        try:
            with open(self.arquivo_log, "a", encoding="utf-8") as f:
                f.write(linha + "\n")
        except:
            pass
        
        # Remove caracteres especiais para print seguro
        linha_print = linha.encode('ascii', 'ignore').decode('ascii')
        print(linha_print)

logs = SistemaLogs()

# ==================================================
# FUNCOES DE CONEXAO SQL SERVER
# ==================================================
def criar_conexao_sqlserver(database=None):
    """Cria conexao com SQL Server usando pymssql"""
    try:
        config = SQLSERVER_CONFIG.copy()
        db_alvo = database if database else BANCO_SQLSERVER
        server_str = f"{config['server']}:{config['port']}"
        
        conexao = pymssql.connect(
            server=server_str,
            user=config['user'],
            password=config['password'],
            database=db_alvo,
            timeout=config['timeout'],
            login_timeout=config['login_timeout']
        )
        return conexao
        
    except Exception as e:
        logs.log(f"ERRO SQL: {str(e)[:50]}", "ERROR")
        return None

def testar_conexao_sqlserver():
    """Testa conectividade com SQL Server"""
    try:
        conexao = criar_conexao_sqlserver()
        if conexao:
            conexao.close()
            return True
        return False
    except:
        return False

# ==================================================
# FUNCAO DE CONSULTA - SOMA DOS VALORES DAS PARCELAS
# ==================================================
def consultar_soma_valores_por_empresa(id_empresa):
    """Consulta SQL Server: soma parcelas situacao=1 por cliente"""
    conexao = None
    cursor = None
    
    try:
        # Validacao do ID
        try:
            id_int = int(id_empresa)
            if id_int <= 0:
                return {"erro": "id_invalido", "detalhes": f"ID invalido: {id_empresa}"}
        except ValueError:
            return {"erro": "id_invalido", "detalhes": f"ID nao numerico: {id_empresa}"}
        
        # Conecta ao SQL Server
        conexao = criar_conexao_sqlserver(BANCO_SQLSERVER)
        if not conexao:
            return {"erro": "falha_conexao_sqlserver"}
        
        cursor = conexao.cursor()
        
        # Query com JOIN
        query = f"""
        SELECT SUM(p.{COLUNA_VALOR_TOTAL}) as total
        FROM {TABELA_PARCELA} p
        INNER JOIN {TABELA_TITULO} t ON t.financeirotitulo_id = p.financeirotitulo_id
        WHERE t.{COLUNA_EMPRESA_ID} = %s
        AND p.{COLUNA_SITUACAO_ID} = 1
        """
        
        logs.log(f"Query ID={id_int}", "DEBUG")
        cursor.execute(query, (id_int,))
        
        resultado = cursor.fetchone()
        soma_total = float(resultado[0] if resultado and resultado[0] is not None else 0.0)
        
        logs.log(f"Soma: R$ {soma_total:.2f} ID={id_int}", "INFO")
        
        return {
            "status": "sucesso",
            "id_empresa": id_empresa,
            "soma_total": soma_total,
            "tabela_principal": TABELA_TITULO,
            "tabela_parcela": TABELA_PARCELA,
            "coluna_somada": COLUNA_VALOR_TOTAL,
            "filtro_situacao": f"{COLUNA_SITUACAO_ID} = 1"
        }
        
    except Exception as e:
        logs.log(f"Erro consulta: {str(e)[:50]}", "ERROR")
        return {"erro": "erro_consulta", "detalhes": str(e)}
    finally:
        if cursor:
            cursor.close()
        if conexao:
            conexao.close()

# ==================================================
# GERENCIADOR FIREBASE LOVABLE
# ==================================================
class GerenciadorFirebaseLovable:
    def __init__(self):
        self.base_url = FIREBASE_URL
        self.em_processamento = False
        self.ultimo_etag = None
        self.tentativas_falhas = 0
        self.max_tentativas_falhas = 3
        
    def _requisicao(self, metodo="GET", caminho="", dados=None):
        """Faz requisicao HTTP para Firebase"""
        
        for tentativa in range(3):
            try:
                url = f"{self.base_url}{caminho}.json"
                headers = {}
                timeout = 10 + (tentativa * 10)
                
                if self.ultimo_etag and metodo == "GET":
                    headers['If-None-Match'] = self.ultimo_etag
                
                if metodo == "GET":
                    resposta = requests.get(url, headers=headers, timeout=timeout)
                elif metodo == "PUT":
                    resposta = requests.put(url, json=dados, headers=headers, timeout=timeout)
                else:
                    resposta = requests.post(url, json=dados, headers=headers, timeout=timeout)
                
                if metodo == "GET" and 'ETag' in resposta.headers:
                    self.ultimo_etag = resposta.headers['ETag']
                
                self.tentativas_falhas = 0
                return resposta
                
            except requests.exceptions.Timeout:
                logs.log(f"Timeout {tentativa+1}/3", "WARNING")
                if tentativa == 2:
                    self.tentativas_falhas += 1
                    return None
                time.sleep(2 ** tentativa)
                
            except Exception as e:
                if tentativa == 2:
                    logs.log(f"Erro req: {str(e)[:30]}", "ERROR")
                    self.tentativas_falhas += 1
                    return None
                time.sleep(2)
        
        return None
    
    def _reconectar(self):
        """Tenta reconectar ao Firebase"""
        logs.log("Reconectando Firebase...", "INFO")
        
        for tentativa in range(1, RECONNECT_MAX_ATTEMPTS + 1):
            try:
                resposta = requests.get(f"{self.base_url}/.json", timeout=10)
                if resposta.status_code == 200:
                    logs.log("Reconectado!", "SUCCESS")
                    self.ultimo_etag = None
                    self.tentativas_falhas = 0
                    return True
            except:
                pass
            
            if tentativa < RECONNECT_MAX_ATTEMPTS:
                time.sleep(RECONNECT_DELAY)
        
        logs.log("Falha reconexao", "ERROR")
        return False
    
    def monitorar_consultas(self):
        """Monitora o no /consulta2/request do Firebase"""
        logs.log("Monitorando /consulta2/request...", "INFO")
        
        while True:
            try:
                if self.tentativas_falhas >= self.max_tentativas_falhas:
                    if not self._reconectar():
                        time.sleep(10)
                        continue
                
                resposta = self._requisicao("GET", "/request")
                
                if not resposta:
                    time.sleep(2)
                    continue
                
                if resposta.status_code == 304:
                    time.sleep(1)
                    continue
                
                if resposta.status_code == 200:
                    dados = resposta.json()
                    self.tentativas_falhas = 0
                    
                    if (dados and isinstance(dados, dict) and
                        dados.get("status") == "processing" and
                        not self.em_processamento):
                        
                        id_empresa = dados.get("id_cli")
                        
                        if id_empresa and id_empresa not in [0, "0", None, ""]:
                            logs.log(f"Consulta ID: {id_empresa}", "SUCCESS")
                            
                            self.em_processamento = True
                            threading.Thread(
                                target=self.processar_consulta,
                                args=(str(id_empresa),),
                                daemon=True
                            ).start()
                
                time.sleep(0.5 if self.em_processamento else 1)
                    
            except Exception as e:
                logs.log(f"Erro monitor: {str(e)[:30]}", "ERROR")
                self.tentativas_falhas += 1
                time.sleep(2)
    
    def processar_consulta(self, id_empresa):
        """Processa consulta no SQL Server e atualiza Firebase"""
        try:
            logs.log(f"Processando ID: {id_empresa}", "INFO")
            resultado = consultar_soma_valores_por_empresa(id_empresa)
            
            if "erro" in resultado:
                erro = resultado["erro"]
                if erro == "id_invalido":
                    self._atualizar_status("erro", resultado.get("detalhes", ""))
                else:
                    self._atualizar_status("timeout", erro)
            else:
                self._enviar_resultado_sucesso(id_empresa, resultado["soma_total"])
        
        except Exception as e:
            logs.log(f"Erro proc: {str(e)[:30]}", "ERROR")
            self._atualizar_status("timeout", "erro_interno")
        
        finally:
            self.em_processamento = False
    
    def _atualizar_status(self, status, mensagem):
        """Atualiza status no Firebase"""
        try:
            self._requisicao("PUT", "/request/status", status)
            self._requisicao("PUT", "/request/message", mensagem[:200])
            logs.log(f"Status: {status}", "INFO")
        except Exception as e:
            logs.log(f"Erro status: {str(e)[:30]}", "ERROR")
    
    def _enviar_resultado_sucesso(self, id_empresa, soma_total):
        """Envia resultado da soma para Firebase"""
        try:
            dados_resultado = {
                "id_empresa": id_empresa,
                "soma_total": soma_total,
                "tabela_principal": TABELA_TITULO,
                "tabela_parcela": TABELA_PARCELA,
                "coluna_somada": COLUNA_VALOR_TOTAL,
                "filtro_situacao": f"{COLUNA_SITUACAO_ID} = 1",
                "timestamp": datetime.now().isoformat()
            }
            
            self._requisicao("PUT", "/result", dados_resultado)
            self._requisicao("PUT", "/request/status", "ok")
            self._requisicao("PUT", "/request/message", "")
            
            logs.log(f"Sucesso! Soma: R$ {soma_total:.2f}", "SUCCESS")
            
        except Exception as e:
            logs.log(f"Erro envio: {str(e)[:30]}", "ERROR")
            self._atualizar_status("timeout", "falha_envio")

# ==================================================
# INICIALIZACAO
# ==================================================
def mostrar_cabecalho():
    """Mostra cabecalho simplificado"""
    print("\n" + "="*50)
    print(" SERVIDOR SQL SERVER - RASPBERRY PI")
    print("="*50)
    print(f"Firebase: /LOVABLE/consulta2")
    print(f"SQL: {SQLSERVER_CONFIG['server']}")
    print(f"Banco: {BANCO_SQLSERVER}")
    print(f"Tabelas: {TABELA_TITULO} / {TABELA_PARCELA}")
    print(f"Coluna soma: {COLUNA_VALOR_TOTAL}")
    print("="*50 + "\n")

def verificar_dependencias():
    """Verifica dependencias"""
    try:
        import pymssql
        logs.log("pymssql OK", "SUCCESS")
        return True
    except ImportError:
        logs.log("pymssql nao encontrado", "WARNING")
        print("\nInstalar pymssql? (s/n)")
        if input().lower() == 's':
            import subprocess
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "pymssql"])
                print("Instalado! Reinicie.")
                time.sleep(2)
                return True
            except:
                print("Falha instalacao")
        return False

def main():
    """Funcao principal"""
    try:
        if not verificar_dependencias():
            sys.exit(1)
        
        mostrar_cabecalho()
        
        # Teste Firebase
        try:
            requests.get(f"{FIREBASE_URL}/.json", timeout=10)
            logs.log("Firebase OK", "SUCCESS")
        except Exception as e:
            logs.log(f"Firebase: {str(e)[:30]}", "WARNING")
        
        # Teste SQL Server
        if testar_conexao_sqlserver():
            logs.log("SQL Server OK", "SUCCESS")
        else:
            logs.log("SQL Server indisponivel", "WARNING")
        
        logs.log("Iniciando servico...", "INFO")
        gerenciador = GerenciadorFirebaseLovable()
        gerenciador.monitorar_consultas()
        
    except KeyboardInterrupt:
        logs.log("Servidor finalizado", "INFO")
        print("\nEncerrado.")
    except Exception as e:
        logs.log(f"Erro fatal: {str(e)[:50]}", "CRITICAL")
        time.sleep(10)
        main()

if __name__ == "__main__":
    main()
