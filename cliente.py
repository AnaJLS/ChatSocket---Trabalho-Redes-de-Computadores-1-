"""
cliente.py — ChatSocket (Trabalho RC1 - Sockets)

Cliente TCP para o chat ChatSocket. Responsável por:
  - conectar-se ao servidor e realizar o handshake inicial de apelido;
  - enviar mensagens digitadas pelo usuário;
  - receber, em uma thread separada, mensagens de outros usuários em
    paralelo ao envio (já que a leitura do teclado é uma operação bloqueante).
"""
import socket
import threading
import sys

HOST = '127.0.0.1'  
PORT = 5000         
BUFFER = 1024       

conectado = True


def receber_mensagens(sock: socket.socket):
    global conectado
    while conectado:
        try:
            dados = sock.recv(BUFFER)
            if not dados:
                print("\n[INFO] Conexão encerrada pelo servidor.")
                conectado = False
                break
            mensagem = dados.decode('utf-8').strip()
            print(f"\r{mensagem}")
            print("Você: ", end='', flush=True)
        except ConnectionResetError:
            if conectado:
                print("\n[INFO] Servidor desconectado inesperadamente.")
            conectado = False
            break
        except OSError:
            break
        except Exception as e:
            if conectado:
                print(f"\n[ERRO] Ao receber mensagem: {e}")
            conectado = False
            break


def conectar_ao_servidor() -> socket.socket | None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
        return sock
    except ConnectionRefusedError:
        print(f"[ERRO] Servidor não encontrado em {HOST}:{PORT}")
        print("  → Verifique se o servidor está rodando.")
        return None
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar: {e}")
        return None


def obter_apelido() -> str:
    while True:
        apelido = input("Digite seu apelido: ").strip()
        if apelido:
            if len(apelido) > 20:
                print("[AVISO] Apelido muito longo. Use até 20 caracteres.")
            else:
                return apelido
        else:
            print("[AVISO] Apelido não pode ser vazio.")


def main():
    global conectado

    print("=" * 50)
    print("   ChatSocket - Cliente TCP")
    print("=" * 50)

    global HOST
    if len(sys.argv) > 1:
        HOST = sys.argv[1]

    print(f"  Servidor: {HOST}:{PORT}")
    print("=" * 50)

    print("Conectando ao servidor...")
    sock = conectar_ao_servidor()
    if not sock:
        sys.exit(1)

    try:
        sinal = sock.recv(BUFFER).decode('utf-8')
        if sinal == "NICK":
            apelido = obter_apelido()
            sock.send(apelido.encode('utf-8'))
        else:
            # Handshake inesperado: seguimos com um apelido padrão em vez
            # de deixar a variável indefinida (evitaria um NameError abaixo).
            apelido = "Desconhecido"
            print(f"[AVISO] Resposta inesperada do servidor no handshake: {sinal!r}")
    except Exception as e:
        print(f"[ERRO] Falha no handshake: {e}")
        sock.close()
        sys.exit(1)

    print(f"\nConectado! Olá, {apelido}! 🎉")
    print("Digite suas mensagens e pressione Enter para enviar.")
    print("Use /ajuda para ver os comandos disponíveis.")
    print("-" * 50)

    thread_recv = threading.Thread(
        target=receber_mensagens,
        args=(sock,),
        daemon=True
    )
    thread_recv.start()

    try:
        while conectado:
            print("Você: ", end='', flush=True)
            try:
                mensagem = input()
            except EOFError:
                break

            if not mensagem.strip():
                continue

            try:
                sock.send(mensagem.encode('utf-8'))
            except Exception as e:
                print(f"[ERRO] Não foi possível enviar: {e}")
                break

            if mensagem.strip().lower() == '/sair':
                conectado = False
                break

    except KeyboardInterrupt:
        print("\n[INFO] Encerrando...")

    conectado = False
    try:
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
    except Exception:
        pass

    print("[INFO] Desconectado. Até logo! 👋")


if __name__ == '__main__':
    main()
