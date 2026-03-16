# Cruzador de Dados

Plataforma para cruzamento e análise de planilhas de vendas e leads. Permite identificar compradores em comum entre produtos, analisar a sequência temporal das compras e exportar os resultados.

## Requisitos

- Python 3.11+

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração de credenciais

O app usa `st.secrets` como fonte principal de credenciais — o mesmo mecanismo do Streamlit Community Cloud. Para desenvolvimento local, crie o arquivo `.streamlit/secrets.toml`:

```bash
cp secrets.toml.example .streamlit/secrets.toml
```

Edite `.streamlit/secrets.toml` com seus usuários e senha. O arquivo já está no `.gitignore` e nunca será commitado.

O login padrão do exemplo é:
- **Usuário:** `admin`
- **Senha:** `admin123`

> **Fallback:** se `.streamlit/secrets.toml` não existir, o app tenta ler `config.yaml` (formato legado).

## Como rodar localmente

```bash
streamlit run app.py
```

Acesse no navegador: `http://localhost:8501`

## Deploy no Streamlit Community Cloud

1. Faça push do repositório para o GitHub (o arquivo de secrets **não vai** junto — está no `.gitignore`)
2. Acesse [share.streamlit.io](https://share.streamlit.io) e conecte o repositório
3. Em **App settings → Secrets**, cole o conteúdo do `secrets.toml.example` adaptado com seus usuários reais:

```toml
[credentials.usernames.admin]
email = "admin@suaempresa.com"
name = "Administrador"
password = "sua_senha_aqui"

[cookie]
expiry_days = 7
key = "string_aleatoria_longa_e_unica"
name = "cruzador_session"
```

4. Faça o deploy — o app lerá as credenciais diretamente do painel de secrets, sem precisar de arquivo local.

> **Dica de segurança:** troque o valor de `cookie.key` por uma string aleatória longa e única para cada ambiente (local e produção).

## Gerenciar usuários

Para adicionar usuários, edite `.streamlit/secrets.toml` (local) ou o painel de Secrets do Streamlit Cloud, seguindo o modelo do `secrets.toml.example`:

```toml
[credentials.usernames.novo_usuario]
email = "novo@empresa.com"
name = "Nome Completo"
password = "senha_aqui"   # será hasheada automaticamente no primeiro login
```

## Como usar

### 1. Carregar dados
No painel lateral, clique em **Carregar planilhas CSV** e selecione um ou mais arquivos. O tipo (vendas ou leads) é detectado automaticamente pelas colunas. Múltiplos arquivos do mesmo tipo são mesclados, removendo duplicatas pelo ID de transação.

### 2. Visão Geral
Métricas gerais dos dados de vendas (total de transações, compradores únicos, período) e gráfico dos 20 produtos com mais transações.

### 3. Cruzamento de Produtos
1. Selecione o **Grupo A** — um ou mais produtos de origem
2. Selecione o **Produto B** — o produto de destino
3. Defina o filtro de **Status** desejado
4. Clique em **Analisar**

Resultado: funil de conversão, sequência de compra (quem comprou A antes ou depois de B), tabela exportável e timeline visual.

### 4. Tabela de Vendas
Visualize e filtre todas as transações por status, produto, estado, método de pagamento e período. Exportação em CSV.

### 5. Tabela de Leads
Visualize e filtre todos os leads por tag, formulário de origem, UTM source/campaign/medium e período. Exportação em CSV.

## Estrutura do projeto

```
cruzador/
├── app.py                    # Interface Streamlit e autenticação
├── requirements.txt
├── secrets.toml.example      # Modelo de credenciais (versionado)
├── config.yaml.example       # Modelo legado (fallback local)
├── .streamlit/
│   └── secrets.toml          # Credenciais locais (não versionado)
└── core/
    ├── loader.py             # Carregamento, detecção de tipo e normalização de CSV
    ├── analyzer.py           # Lógica de cruzamento e análise de sequência
    └── charts.py             # Gráficos Plotly
```

## Identificação de compradores

O comprador é identificado pela combinação de **CPF/CNPJ + Nome**. Quando o CPF não está preenchido, o sistema usa somente o nome normalizado como chave. Isso garante que a mesma pessoa seja reconhecida entre compras diferentes no mesmo arquivo ou em arquivos distintos.
