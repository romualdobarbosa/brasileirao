# Oracle Cloud: do zero ao link público

Passo a passo pra subir o serving numa VM Always Free da Oracle: Postgres, Metabase e Caddy em Docker Compose, com HTTPS num subdomínio do DuckDNS. O desenho está na [spec do serving](../../SPEC-serving.md).

Onde aparece `<ip-da-vm>`, `<subdominio>` ou `<senha>`, troque pelo valor real. Nenhum desses valores entra no git: eles ficam no `.env` (local e da VM) e no `~/.ssh/config`.

## 1. Conta e home region

1. Criar a conta em <https://signup.cloud.oracle.com>. Pede cartão, mas o que é Always Free não é cobrado.
2. **Home region:** escolher **Brazil East (São Paulo)** ou **Brazil Southeast (Vinhedo)**. Ela não muda depois. Se der, ver antes em fóruns/relatos recentes qual das duas está com capacidade de A1.

## 2. Pay As You Go e orçamento

Conta paga não sofre a recuperação de VM ociosa (a Oracle recupera VM Always Free de conta gratuita quando o 95º percentil de CPU fica abaixo de 20% por 7 dias), e o que é Always Free continua de graça.

1. **Billing & Cost Management → Upgrade and Manage Payment → Upgrade to Pay As You Go.** A aprovação pode levar algumas horas.
2. **Billing & Cost Management → Budgets → Create Budget:**
   - Target: o compartimento raiz (a conta toda).
   - Valor mensal: o menor que o console aceitar (meta: US$ 1).
   - Alert rule: tipo **Actual Spend**, limite em porcentagem, o menor aceito (meta: 1%), com o seu e-mail.

O alerta só avisa, não desliga nada.

### Quando chegar um e-mail de custo

1. **Billing & Cost Management → Cost Analysis:** agrupar por **Service** e depois por **Resource** pra achar o que gerou custo.
2. Se for a VM: **Compute → Instances →** a instância **→ Stop** (para de cobrar CPU e memória; o disco continua). Se for outro recurso fora da cota (volume extra, IP reservado, balanceador), apagar.
3. Conferir de novo no dia seguinte em Cost Analysis.

## 3. Chave SSH (na sua máquina)

```bash
ssh-keygen -t ed25519 -f ~/.ssh/oracle_brasileirao -C brasileirao-vm
cat ~/.ssh/oracle_brasileirao.pub   # vai no passo 4
```

## 4. VM A1

1. **Governance & Administration → Limits, Quotas and Usage:** filtrar por `A1` e anotar o limite Always Free de OCPU e memória vigente (relatos de 2026 falam em corte de 4 OCPU / 24 GB pra 2 OCPU / 12 GB).
2. **Compute → Instances → Create instance:**
   - Image: **Canonical Ubuntu 24.04** (a versão aarch64, que aparece com o shape A1).
   - Shape: **VM.Standard.A1.Flex**, **2 OCPU / 12 GB** se couber no limite (Postgres + Metabase cabem em 1 OCPU / 6 GB).
   - Networking: criar uma VCN nova com subnet pública e **Assign a public IPv4 address** marcado.
   - SSH keys: **Paste public keys**, colando o `oracle_brasileirao.pub`.
   - Boot volume: o padrão (50 GB) está dentro da cota.
3. Se der **"Out of capacity"**: tentar outro Availability Domain (se houver), ou de novo em outro horário. Pay As You Go costuma ajudar.
4. Anotar o IP público da instância.

Na sua máquina, `~/.ssh/config`:

```
Host brasileirao-vm
  HostName <ip-da-vm>
  User ubuntu
  IdentityFile ~/.ssh/oracle_brasileirao
  IdentitiesOnly yes
```

```bash
ssh brasileirao-vm 'uname -m'   # aarch64
ssh brasileirao-vm 'sudo sshd -T | grep -E "^(passwordauthentication|permitrootlogin)"'
# esperado: passwordauthentication no / permitrootlogin no (ou without-password)
```

## 5. Security list: só 22, 80 e 443

**Networking → Virtual Cloud Networks →** a VCN **→ Security Lists → Default Security List:**

- Ingress já vem com TCP 22 de `0.0.0.0/0` e duas regras de ICMP. Deixar.
- **Add Ingress Rules:** TCP, origem `0.0.0.0/0`, porta de destino `80`; outra igual com `443`.
- Não abrir mais nada. A 5432 e a 3000 nem escutam fora da VM (o compose publica só em `127.0.0.1`), mas também não abrem aqui.

## 6. `iptables` da imagem Ubuntu da Oracle

A imagem vem com regras de `iptables` que rejeitam tudo que não é SSH, mesmo com a security list aberta. Fazer **antes** de instalar o Docker, pra não salvar as regras do Docker junto.

```bash
ssh brasileirao-vm
sudo iptables -L INPUT --line-numbers -n
# achar o número da linha "REJECT all ... reject-with icmp-host-prohibited" (ex.: 6)
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save
sudo iptables -L INPUT -n | grep -E "dpt:(80|443)"   # as duas regras antes do REJECT
```

## 7. Docker com o plugin compose, ligado no boot

Na VM (repositório oficial do Docker, que tem pacote `arm64`):

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl rsync
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker containerd
sudo usermod -aG docker ubuntu
exit   # sair e entrar de novo pra valer o grupo docker
```

```bash
ssh brasileirao-vm 'docker compose version && systemctl is-enabled docker'   # ... enabled
```

Os serviços do compose têm `restart: unless-stopped`: com o Docker ligado no boot, voltam sozinhos depois de um reboot.

## 8. DuckDNS

1. Entrar em <https://www.duckdns.org> (login com GitHub ou Google).
2. Criar o subdomínio (`<subdominio>.duckdns.org`).
3. No campo **current ip** do subdomínio, colocar o IP público da VM e **update ip**.
4. Se um dia o IP da VM mudar (ex.: instância recriada), repetir o passo 3.

Conferir na sua máquina antes de seguir (o Caddy pede o certificado assim que sobe, e o Let's Encrypt limita tentativas que falham):

```bash
dig +short <subdominio>.duckdns.org   # tem que devolver <ip-da-vm>
```

## 9. `.env` da VM

Criado à mão, uma vez. O `make deploy` nunca sobrescreve nem apaga esse arquivo.

```bash
ssh brasileirao-vm
mkdir -p ~/brasileirao
umask 077
cat > ~/brasileirao/.env <<EOF
POSTGRES_PASSWORD=$(openssl rand -hex 24)
PG_SENHA_CARGA=$(openssl rand -hex 24)
PG_SENHA_LEITOR=$(openssl rand -hex 24)
PG_SENHA_METABASE=$(openssl rand -hex 24)
DOMINIO=<subdominio>.duckdns.org
EOF
grep -E '^(PG_SENHA_CARGA|PG_SENHA_LEITOR)=' ~/brasileirao/.env   # anotar no gerenciador de senhas
```

As senhas são definidas pelo `initdb/` só na **primeira** subida do Postgres (volume vazio). Trocar no `.env` depois não muda a senha no banco.

No `.env` **local** (raiz do projeto, modelo em [`serving/.env.example`](../../serving/.env.example)):

```bash
VM_HOST=brasileirao-vm          # o Host do ~/.ssh/config
VM_USUARIO=ubuntu
VM_PG_SENHA_CARGA=<a PG_SENHA_CARGA do .env da VM>
```

`make carga-remota` usa a `VM_PG_SENHA_CARGA` no lugar da `BRASILEIRAO_PG_SENHA`, então o `.env` local continua apontando pro Postgres local na `make carga`.

## 10. Primeiro deploy

Na sua máquina, na raiz do projeto:

```bash
make deploy
```

Ele copia `serving/` pra `~/brasileirao` da VM por `rsync` (sem `.env`) e roda `docker compose --profile publico up -d --wait` lá. A primeira vez demora: baixa as imagens e o Metabase leva um ou dois minutos pra ficar saudável.

```bash
ssh brasileirao-vm 'cd brasileirao && docker compose ps'
ssh brasileirao-vm 'cd brasileirao && docker compose logs caddy | grep -i "certificate obtained"'
```

Se o certificado não vier, ver o log do Caddy. As causas comuns são o DuckDNS apontando pro IP errado ou a 80 fechada (passos 5 e 6). Nesse caso, parar o Caddy (`docker compose stop caddy`) até corrigir, pra não estourar o limite do Let's Encrypt.

**Logo em seguida**, abrir `https://<subdominio>.duckdns.org` e criar o admin do Metabase com senha forte: até isso ser feito, a tela de configuração inicial fica aberta pra qualquer um. Depois seguir o [dashboard.md](dashboard.md) ("Como montar"), que tira o Sample Database e cadastra o banco `brasileirao` como `leitor`. Em **Admin → Authentication**, conferir que não há cadastro aberto (só convite).

## 11. Carga remota

Depois de `make gold`:

```bash
make carga-remota
```

Abre um túnel SSH (`localhost:15432` → `127.0.0.1:5432` da VM), roda a carga por ele e fecha o túnel mesmo se a carga falhar. As contagens de cada tabela saem no log.

Pra conferir que nenhum túnel ficou pra trás:

```bash
ss -tln | grep 15432 || echo "túnel fechado"
```

## 12. Conferência de fora

Da sua máquina (ou de outra rede):

```bash
nc -zv -w 5 <ip-da-vm> 22    # succeeded
nc -zv -w 5 <ip-da-vm> 80    # succeeded
nc -zv -w 5 <ip-da-vm> 443   # succeeded
nc -zv -w 5 <ip-da-vm> 5432  # timeout ou refused
nc -zv -w 5 <ip-da-vm> 3000  # timeout ou refused
curl -sI https://<subdominio>.duckdns.org | head -1   # HTTP/2 200 (ou 302)
```

E o reboot:

```bash
ssh brasileirao-vm 'sudo reboot'
# esperar uns minutos
curl -sI https://<subdominio>.duckdns.org | head -1
ssh brasileirao-vm 'cd brasileirao && docker compose ps'   # os 3 serviços up
```

## 13. Backup do Metabase

O banco `metabase` guarda as perguntas, o dashboard e os usuários. Depois de montar o dashboard (e a cada mudança grande):

```bash
make backup-metabase   # backups/metabase-AAAA-MM-DD.sql.gz, fora do git
```

Pra restaurar numa VM nova, o banco `metabase` precisa estar vazio, ou seja, antes de o Metabase subir pela primeira vez. Em vez do `make deploy` do passo 10:

```bash
rsync -av --exclude .env serving/ brasileirao-vm:brasileirao/
ssh brasileirao-vm 'cd brasileirao && docker compose up -d --wait postgres'
gunzip -c backups/metabase-AAAA-MM-DD.sql.gz \
  | ssh brasileirao-vm 'cd brasileirao && docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d metabase'
make deploy
```

## Atualizar depois

- Mudou algo em `serving/` (tag de imagem, Caddyfile): `make deploy`.
- Gold novo: `make gold && make carga-remota`.
