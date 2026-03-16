# Cruzador de Dados

Plataforma local para cruzamento e análise de planilhas de vendas. Permite identificar compradores em comum entre produtos, analisar a sequência temporal das compras e exportar os resultados.

## Requisitos

- Python 3.11+

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração inicial

Antes de rodar pela primeira vez, copie o arquivo de configuração de exemplo:

```bash
cp config.yaml.example config.yaml   # Linux/macOS
copy config.yaml config.yaml         # Windows
```

> `config.yaml` contém credenciais e **não deve ser versionado**. Ele já está no `.gitignore`.

## Como rodar

```bash
streamlit run app.py
```

Acesse no navegador: `http://localhost:8501`

## Como usar

### 1. Carregar dados
No painel lateral, clique em **Carregar planilhas CSV** e selecione um ou mais arquivos. Múltiplos arquivos são mesclados automaticamente, removendo transações duplicadas pelo ID.

### 2. Visão Geral
A aba **Visão Geral** exibe métricas gerais dos dados carregados (total de transações, compradores únicos, período) e o gráfico dos 20 produtos com mais transações. Use o filtro de Status para considerar apenas vendas com status desejado (ex: COMPLETO).

### 3. Cruzamento de Produtos
Na aba **Cruzamento de Produtos**:

1. Selecione o **Grupo A** — um ou mais produtos de origem (ex: `PACK - 5 Livros Digitais`, `PACK - 6 Livros Digitais`)
2. Selecione o **Produto B** — o produto de destino (ex: `BucoApprove`)
3. Defina o filtro de **Status** desejado
4. Clique em **Analisar**

O resultado mostra:
- **Métricas:** total de compradores por grupo, quantos compraram ambos, taxa de conversão A→B, média de dias entre as compras
- **Funil de conversão:** visualização do número de compradores que foram do Grupo A para o Produto B
- **Sequência de compra:** pizza com a distribuição de quem comprou A antes de B, B antes de A ou na mesma data
- **Tabela de intersecção:** lista detalhada de cada comprador com as datas e a sequência calculada
- **Timeline:** gráfico de linha do tempo mostrando as duas datas de compra por comprador
- **Exportação:** botões para baixar cada segmento (ambos / só A / só B) em CSV

## Gerenciar usuários

Edite `config.yaml` para adicionar ou remover usuários:

```yaml
credentials:
  usernames:
    joao:
      email: joao@empresa.com
      name: João Silva
      password: senha123   # será hasheada automaticamente no primeiro login
```

## Estrutura do projeto

```
cruzador/
├── app.py              # Interface Streamlit e autenticação
├── config.yaml         # Credenciais (não versionado)
├── requirements.txt
└── core/
    ├── loader.py       # Carregamento e normalização de CSV
    ├── analyzer.py     # Lógica de cruzamento e análise de sequência
    └── charts.py       # Gráficos Plotly
```

## Identificação de compradores

O comprador é identificado pela combinação de **CPF/CNPJ + Nome**. Quando o CPF não está preenchido, o sistema usa somente o nome normalizado como chave. Isso garante que a mesma pessoa seja reconhecida entre compras diferentes no mesmo arquivo ou em arquivos distintos.
