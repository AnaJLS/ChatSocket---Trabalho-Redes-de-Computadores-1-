"""
servidor.py — ChatSocket (Trabalho RC1 - Sockets)

Servidor TCP multi-cliente para um chat em tempo real. Responsável por:
  - aceitar conexões de múltiplos clientes simultaneamente (uma thread por cliente);
  - realizar o handshake inicial de apelido (protocolo "NICK");
  - repassar (broadcast) as mensagens de um cliente para todos os demais;
  - processar comandos especiais (/ajuda, /usuarios, /sair);
  - manter a lista de clientes conectados de forma segura entre threads,
    usando um Lock para evitar condições de corrida.
"""
import socket
import threading
import datetime

HOST = '0.0.0.0'   
PORT = 5000         
MAX_CLIENTES = 10   
BUFFER = 1024      

clientes = {}      
lock = threading.Lock()


def log(msg: str):
    agora = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{agora}] {msg}")


def broadcast(mensagem: str, remetente: socket.socket = None):
    # Garante um delimitador de fim de mensagem ('\n'). Como TCP é um fluxo
    # contínuo de bytes (sem separação nativa de mensagens), enviar sempre
    # com o mesmo terminador evita que duas mensagens cheguem "coladas" no
    # cliente quando enviadas em sequência rápida.
    if not mensagem.endswith('\n'):
        mensagem += '\n'
    with lock:
        for cliente in list(clientes.keys()):
            if cliente != remetente:
                try:
                    cliente.send(mensagem.encode('utf-8'))
                except Exception:
                    remover_cliente(cliente)


def remover_cliente(cliente: socket.socket):
    with lock:
        if cliente in clientes:
            apelido = clientes.pop(cliente)
            try:
                cliente.close()
            except Exception:
                pass
            return apelido
    return None


def listar_usuarios() -> str:
    with lock:
        nomes = list(clientes.values())
    if not nomes:
        return "Nenhum usuário online."
    return "Usuários online: " + ", ".join(nomes)


def tratar_cliente(cliente: socket.socket, endereco: tuple):
    log(f"Nova conexão: {endereco}")

    try:
        cliente.send("NICK".encode('utf-8'))
        apelido = cliente.recv(BUFFER).decode('utf-8').strip()

        if not apelido:
            apelido = f"Usuario_{endereco[1]}"

        with lock:
            # Evita dois usuários com o mesmo apelido: se já existir, anexa
            # um sufixo numérico baseado na porta de origem da conexão.
            if apelido in clientes.values():
                apelido = f"{apelido}_{endereco[1]}"
            clientes[cliente] = apelido
            total = len(clientes)

        log(f"'{apelido}' entrou no chat.")
        log(f"Clientes conectados: {total}")
    except Exception as e:
        log(f"Erro ao receber apelido de {endereco}: {e}")
        cliente.close()
        return

    broadcast(f"[SERVIDOR] '{apelido}' entrou na sala! 🎉", remetente=cliente)
    cliente.send(f"[SERVIDOR] Bem-vindo ao ChatSocket, {apelido}! Digite /ajuda para ver os comandos.\n".encode('utf-8'))
    broadcast(f"[SERVIDOR] {listar_usuarios()}", remetente=None)
    
    while True:
        try:
            dados = cliente.recv(BUFFER)
            if not dados:
                break

            mensagem = dados.decode('utf-8').strip()

            if mensagem.startswith('/'):
                processar_comando(cliente, apelido, mensagem)
            else:
                agora = datetime.datetime.now().strftime("%H:%M")
                msg_formatada = f"[{agora}] {apelido}: {mensagem}"
                log(msg_formatada)
                broadcast(msg_formatada, remetente=cliente)
                cliente.send(f"[{agora}] Você: {mensagem}\n".encode('utf-8'))

        except ConnectionResetError:
            break
        except Exception as e:
            log(f"Erro com '{apelido}': {e}")
            break

    apelido_saiu = remover_cliente(cliente)
    if apelido_saiu:
        log(f"'{apelido_saiu}' saiu do chat.")
        broadcast(f"[SERVIDOR] '{apelido_saiu}' saiu da sala. 👋")


def processar_comando(cliente: socket.socket, apelido: str, comando: str):
    cmd = comando.lower().split()[0]

    if cmd == '/ajuda':
        ajuda = (
            "\n[SERVIDOR] ── Comandos disponíveis ──\n"
            "  /ajuda       → Exibe esta mensagem\n"
            "  /usuarios    → Lista usuários online\n"
            "  /sair        → Sai do chat\n"
            "──────────────────────────────────\n"
        )
        cliente.send(ajuda.encode('utf-8'))

    elif cmd == '/usuarios':
        cliente.send(f"[SERVIDOR] {listar_usuarios()}\n".encode('utf-8'))

    elif cmd == '/sair':
        cliente.send("[SERVIDOR] Até logo! 👋\n".encode('utf-8'))
        raise ConnectionResetError("Cliente solicitou saída.")

    else:
        cliente.send(f"[SERVIDOR] Comando desconhecido: '{comando}'. Digite /ajuda.\n".encode('utf-8'))


def iniciar_servidor():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        servidor.bind((HOST, PORT))
    except OSError as e:
        print(f"[ERRO] Não foi possível iniciar na porta {PORT}: {e}")
        return

    servidor.listen(MAX_CLIENTES)
    print("=" * 50)
    print("   ChatSocket - Servidor TCP Iniciado")
    print("=" * 50)
    log(f"Servidor escutando em {HOST}:{PORT}")
    log(f"Máximo de clientes: {MAX_CLIENTES}")
    print("=" * 50)
    print("  Aguardando conexões... (Ctrl+C para parar)\n")

    try:
        while True:
            try:
                cliente_sock, endereco = servidor.accept()

                with lock:
                    cheio = len(clientes) >= MAX_CLIENTES
                if cheio:
                    log(f"Conexão recusada de {endereco}: limite de {MAX_CLIENTES} clientes atingido.")
                    try:
                        cliente_sock.send("[SERVIDOR] Servidor cheio. Tente novamente mais tarde.\n".encode('utf-8'))
                        cliente_sock.close()
                    except Exception:
                        pass
                    continue

                thread = threading.Thread(
                    target=tratar_cliente,
                    args=(cliente_sock, endereco),
                    daemon=True
                )
                thread.start()
            except Exception as e:
                log(f"Erro ao aceitar conexão: {e}")

    except KeyboardInterrupt:
        print("\n[SERVIDOR] Encerrando servidor...")
    finally:
        broadcast("[SERVIDOR] O servidor está sendo encerrado. Até logo!")
        with lock:
            for c in list(clientes.keys()):
                try:
                    c.close()
                except Exception:
                    pass
        servidor.close()
        print("[SERVIDOR] Servidor encerrado.")


if __name__ == '__main__':
    iniciar_servidor()
