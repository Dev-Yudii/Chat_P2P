# Chat P2P Distribuído — Interface Web

> **Projeto Acadêmico — Sistemas Distribuídos**
> Python 3.8+ | Flask | Socket.IO | Rede P2P real sem servidor central

---

## O que é este programa?

É um sistema de **chat em tempo real totalmente descentralizado**. Diferente do WhatsApp ou Telegram, não existe nenhum servidor central. Cada computador rodando o programa é ao mesmo tempo cliente e servidor — ele manda mensagens *e* recebe conexões de outros ao mesmo tempo.

Abra um navegador em qualquer computador da rede, e você verá uma interface de chat onde as mensagens trafegam diretamente entre os participantes.

---

## Conceitos de Sistemas Distribuídos implementados

| Conceito | Como aparece no código |
|---|---|
| **Comunicação P2P via TCP** | Cada peer abre um `ServerSocket` e aceita conexões diretas de outros nós |
| **Flooding controlado** | Mensagens se propagam em cascata; campo `hops` (TTL) limita a profundidade |
| **Deduplicação por UUID** | Cada mensagem tem um `id` único; nós ignoram mensagens já vistas |
| **Gossip Protocol** | Ao conectar, peers trocam listas de nós conhecidos (PEER_EXCHANGE) |
| **Eleição de líder** | Algoritmo simples: maior porta = líder; eleição propagada via flooding |
| **Failure Detector** | PING/PONG periódico com timeout; nó morto é removido automaticamente |
| **Auto-descoberta UDP** | Beacon broadcast UDP a cada 5s; peers aparecem sem digitação |
| **Reconexão automática** | Loop que tenta reestabelecer conexão com nós conhecidos offline |
| **Histórico persistente** | Cada nó grava seu histórico de mensagens em JSON local |
| **Interface web via WebSocket** | Browser recebe atualizações em tempo real via Socket.IO |

---

## Instalação

### Pré-requisitos

- Python 3.8 ou superior
- pip

### Instalar dependências

```bash
pip install flask flask-socketio eventlet
```

Ou usando o arquivo de requisitos:

```bash
pip install -r requirements.txt
```

---

## Como executar

### Iniciando peers

Cada peer é uma instância separada do programa. Abra um terminal diferente para cada um.

```bash
# Peer 1 — porta TCP 5001, interface web em http://localhost:6001
python app.py 5001

# Peer 2 — porta TCP 5002, interface web em http://localhost:6002
python app.py 5002

# Peer 3 — porta TCP 5003, interface web em http://localhost:6003
python app.py 5003
```

Acesse `http://localhost:6001` no navegador para ver a interface do peer 1.

### Opções adicionais

```bash
# Definir um apelido
python app.py 5001 --name "Alice"

# Porta web personalizada
python app.py 5001 --web-port 8080
```

### Lógica das portas

| Porta TCP | Porta Web padrão |
|-----------|-----------------|
| 5001 | 6001 |
| 5002 | 6002 |
| 5003 | 6003 |
| N | N + 1000 |

---

## Como conectar os peers

### Opção 1 — Descoberta automática (mesma rede local)

Se os computadores estão na mesma rede Wi-Fi ou LAN, os peers aparecem automaticamente na barra lateral com um botão **"conectar"**. Basta clicar.

O programa envia um beacon UDP broadcast a cada 5 segundos na porta 47777. Qualquer outro peer na rede ouve esse beacon e exibe o nó descoberto na interface.

### Opção 2 — Conexão manual

Use o formulário na barra lateral. Informe o IP do computador alvo e a porta TCP (não a web). Por exemplo: IP `192.168.1.5`, porta `5001`.

Isso é útil para conectar peers em redes diferentes ou quando o broadcast UDP está bloqueado.

---

## Funcionalidades da interface web

### Barra superior
- **Nome do peer atual** — identificador deste nó na rede
- **Indicador de líder** — mostra quem é o líder eleito (`👑 você é o líder` ou `líder: peer_5002`)
- **Contagem de peers** — quantos nós estão conectados agora
- **Status online/offline**

### Barra lateral
- **Lista de nós na rede** — todos os peers conectados e os descobertos via UDP
- Ícone verde = conectado, cinza = descoberto mas não conectado
- Coroa dourada 👑 marca o líder atual
- Botão **"conectar"** em nós não conectados
- **Formulário de conexão manual** para conectar por IP e porta

### Área de chat
- Mensagens em tempo real de todos os peers da rede
- Histórico das últimas 50 mensagens carregado ao abrir
- Mensagens próprias aparecem à direita (azul), as de outros à esquerda
- Notificações de sistema quando alguém entra ou sai
- `Enter` envia; `Shift+Enter` para nova linha

---

## Como funciona a rede por dentro

### Ciclo de vida de uma mensagem

```
Usuário digita → Socket.IO → app.py → PeerManager.broadcast()
                                            │
                       ┌────────────────────┤
                       ▼                    ▼
                    peer_5002           peer_5003
                       │
                       ▼
              peer_5002 reencaminha para seus próprios vizinhos
              (hops+1, UUID já visto = descartado pelos outros)
```

### Fechamento automático da malha

Quando A conecta em B:
1. B envia sua lista de todos os nós que conhece (PEER_EXCHANGE)
2. A conecta diretamente em cada um desses nós
3. Resultado: a rede vira uma malha completa, não uma estrela

```
Antes:  A → B → C        Depois: A ↔ B ↔ C
                                  ↑_______↑
```

Se B cair, A e C ainda se comunicam diretamente.

### Eleição de líder

- Algoritmo: o nó com a **maior porta** é eleito líder
- Quando alguém entra ou sai, todos re-elegem localmente
- O novo líder é anunciado via flooding (MSG `LEADER_ANNOUNCE`) para toda a rede
- A interface atualiza automaticamente o indicador de líder

---

## Estrutura dos arquivos

```
p2p_web/
├── app.py                    ← Servidor Flask + Socket.IO (ponto de entrada)
├── requirements.txt
├── README.md
├── network/
│   └── peer_manager.py       ← Toda a lógica P2P (TCP, UDP, eleição, gossip)
├── models/
│   └── message.py            ← Estrutura e serialização das mensagens (JSON)
├── utils/
│   └── history.py            ← Persistência do histórico em arquivo JSON local
└── templates/
    └── index.html            ← Interface web completa (HTML + CSS + JS)
```

### `app.py`
Servidor web. Recebe mensagens do browser via WebSocket (Socket.IO) e repassa para o `PeerManager`. Quando o `PeerManager` recebe algo da rede P2P, emite eventos Socket.IO para atualizar o browser em tempo real.

### `network/peer_manager.py`
Coração do sistema. Gerencia:
- Servidor TCP para aceitar conexões
- Handshake de identificação entre peers
- Broadcast de mensagens via flooding
- Gossip: troca de tabelas de peers conhecidos
- Heartbeat PING/PONG e detecção de falhas
- Beacon UDP para descoberta automática
- Eleição de líder e propagação do resultado
- Reconexão automática a nós conhecidos

### `models/message.py`
Define o formato JSON de todas as mensagens que trafegam na rede.

### `utils/history.py`
Salva e carrega o histórico de mensagens em um arquivo JSON por nó (`chat_history_5001.json`, etc). Thread-safe.

---

## Formato das mensagens na rede

Todas as mensagens são JSON delimitadas por `\n`:

```json
{
  "id":        "550e8400-e29b-41d4-a716-446655440000",
  "type":      "CHAT",
  "sender":    "peer_5001",
  "message":   "Olá, rede distribuída!",
  "timestamp": "2026-06-07T20:00:00.000000+00:00",
  "hops":      0
}
```

| Tipo | Descrição |
|------|-----------|
| `CHAT` | Mensagem de texto do usuário |
| `JOIN` | Notificação de entrada na sala |
| `LEAVE` | Notificação de saída |
| `PING` | Heartbeat enviado a cada 3s |
| `PONG` | Resposta ao heartbeat |
| `HANDSHAKE` | Identificação ao conectar (port + IP + nome) |
| `PEER_EXCHANGE` | Lista de nós conhecidos (gossip) |
| `LEADER_ANNOUNCE` | Propagação de novo líder eleito |

---

## Limitações conhecidas (pontos de falha em SD)

| Limitação | Impacto | O que seria necessário em produção |
|-----------|---------|-----------------------------------|
| **Eleição por maior porta** | Não reflete capacidade real do nó | Algoritmo Bully ou Raft |
| **Cache de UUIDs em memória** | Limpo ao reiniciar → duplicatas possíveis | Cache persistido em Redis/SQLite |
| **Sem criptografia** | Mensagens em texto puro na rede | TLS com `ssl.wrap_socket()` |
| **Sem autenticação** | Qualquer nó pode entrar | Chaves assimétricas / certificados |
| **Flooding não escala** | O(n×e) mensagens — ruim com muitos nós | Gossip estruturado ou DHT |
| **Ícone às vezes vermelho** | Race condition entre detecção TCP e UI | Lock mais granular + debounce no frontend |
| **UDP broadcast bloqueado** | Descoberta automática falha em algumas redes | mDNS (Bonjour) ou tracker central |

---

## Comparação com sistemas P2P reais

| Característica | Este projeto | BitTorrent | Signal |
|----------------|-------------|------------|--------|
| Descoberta | UDP Broadcast | DHT (Kademlia) | Servidor central |
| Propagação | Flooding | Tracker / DHT | Relay servers |
| Eleição de líder | Maior porta | N/A | N/A |
| Criptografia | ❌ | Opcional | ✅ E2E |
| Escala máxima | ~20 nós | Milhões | Bilhões |
| Tolerância a falhas | Parcial | Alta | Alta |
