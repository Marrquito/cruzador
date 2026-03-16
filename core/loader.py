import io
import re
import pandas as pd


ENCODINGS = ["utf-8", "latin-1", "cp1252"]

# Colunas de data por tipo de arquivo
DATE_COLS_VENDAS = ["Data do Pedido", "Data de Aprovação"]
DATE_COLS_LEADS = ["lead_register"]

# Colunas numéricas de vendas (formato brasileiro com vírgula decimal)
NUMERIC_COLS_VENDAS = [
    "Valor do Produto",
    "Valor Pago pelo Comprador Sem Taxas e Impostos",
    "Taxa Hotmart Total",
]

# Assinaturas para detecção automática do tipo de arquivo
_VENDAS_SIGNATURE = {"ID Transação", "Nome do Produto", "E-mail do Comprador"}
_LEADS_SIGNATURE = {"lead_id", "lead_email", "utm_source"}


# ── Internos ──────────────────────────────────────────────────────────────────

def _try_read(file_bytes: bytes) -> pd.DataFrame:
    for enc in ENCODINGS:
        try:
            return pd.read_csv(io.BytesIO(file_bytes), encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            continue
    raise ValueError("Não foi possível decodificar o arquivo. Tente salvar como UTF-8.")


def _normalize_cpf(value) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"[^0-9]", "", str(value))


def _normalize_name(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _parse_br_number(value) -> float | None:
    """Converte número no formato brasileiro ('1.234,56') para float."""
    if pd.isna(value):
        return None
    s = str(value).strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


# ── Detecção de tipo ──────────────────────────────────────────────────────────

def detect_file_type(df: pd.DataFrame) -> str:
    """Retorna 'vendas', 'leads' ou 'unknown' com base nas colunas do DataFrame."""
    cols = set(df.columns)
    if _VENDAS_SIGNATURE.issubset(cols):
        return "vendas"
    if _LEADS_SIGNATURE.issubset(cols):
        return "leads"
    return "unknown"


# ── Carregamento ──────────────────────────────────────────────────────────────

def load_vendas(file_bytes: bytes) -> pd.DataFrame:
    df = _try_read(file_bytes)

    for col in DATE_COLS_VENDAS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in NUMERIC_COLS_VENDAS:
        if col in df.columns:
            df[col] = df[col].apply(_parse_br_number)

    if "CPF/CNPJ Comprador" in df.columns and "Nome do Comprador" in df.columns:
        df["_cpf_norm"] = df["CPF/CNPJ Comprador"].apply(_normalize_cpf)
        df["_nome_norm"] = df["Nome do Comprador"].apply(_normalize_name)
        df["_id_comprador"] = df.apply(
            lambda r: r["_cpf_norm"] if r["_cpf_norm"] else r["_nome_norm"],
            axis=1,
        )
    return df


def load_leads(file_bytes: bytes) -> pd.DataFrame:
    df = _try_read(file_bytes)

    for col in DATE_COLS_LEADS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def load_csv(file_bytes: bytes) -> pd.DataFrame:
    """Carrega qualquer CSV e aplica o parser adequado ao tipo detectado."""
    df_raw = _try_read(file_bytes)
    file_type = detect_file_type(df_raw)

    if file_type == "vendas":
        return load_vendas(file_bytes)
    if file_type == "leads":
        return load_leads(file_bytes)

    # fallback para arquivo desconhecido: retorna sem tratamento especial
    return df_raw


# ── Merge ─────────────────────────────────────────────────────────────────────

def merge_vendas(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(dfs, ignore_index=True)
    if "ID Transação" in merged.columns:
        merged = merged.drop_duplicates(subset=["ID Transação"])
    return merged


def merge_leads(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(dfs, ignore_index=True)
    if "lead_id" in merged.columns:
        merged = merged.drop_duplicates(subset=["lead_id"])
    return merged


def merge_files(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Compatibilidade com código existente — equivale a merge_vendas."""
    return merge_vendas(dfs)


# ── Helpers de filtro — Vendas ────────────────────────────────────────────────

def get_products(df: pd.DataFrame) -> list[str]:
    if "Nome do Produto" not in df.columns:
        return []
    return sorted(df["Nome do Produto"].dropna().unique().tolist())


def get_status_options(df: pd.DataFrame) -> list[str]:
    if "Status" not in df.columns:
        return []
    return sorted(df["Status"].dropna().unique().tolist())


def get_states(df: pd.DataFrame) -> list[str]:
    if "Estado do Comprador" not in df.columns:
        return []
    return sorted(df["Estado do Comprador"].dropna().unique().tolist())


def get_payment_methods(df: pd.DataFrame) -> list[str]:
    if "Método de Pagamento" not in df.columns:
        return []
    return sorted(df["Método de Pagamento"].dropna().unique().tolist())


# ── Helpers de filtro — Leads ─────────────────────────────────────────────────

def get_tags(df: pd.DataFrame) -> list[str]:
    if "tag_name" not in df.columns:
        return []
    return sorted(df["tag_name"].dropna().unique().tolist())


def get_forms(df: pd.DataFrame) -> list[str]:
    if "lead_register_form" not in df.columns:
        return []
    return sorted(df["lead_register_form"].dropna().astype(str).unique().tolist())


def get_utm_options(df: pd.DataFrame, utm_col: str) -> list[str]:
    if utm_col not in df.columns:
        return []
    return sorted(df[utm_col].dropna().unique().tolist())
