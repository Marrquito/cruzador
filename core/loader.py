import io
import re
import pandas as pd


ENCODINGS = ["utf-8", "latin-1", "cp1252"]

DATE_COLS = ["Data do Pedido", "Data de Aprovação"]


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


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def load_csv(file_bytes: bytes) -> pd.DataFrame:
    df = _try_read(file_bytes)
    df = _parse_dates(df)

    if "CPF/CNPJ Comprador" in df.columns and "Nome do Comprador" in df.columns:
        df["_cpf_norm"] = df["CPF/CNPJ Comprador"].apply(_normalize_cpf)
        df["_nome_norm"] = df["Nome do Comprador"].apply(_normalize_name)
        # Chave composta: prioriza CPF quando disponível, senão usa nome
        df["_id_comprador"] = df.apply(
            lambda r: r["_cpf_norm"] if r["_cpf_norm"] else r["_nome_norm"],
            axis=1,
        )
    return df


def merge_files(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    merged = pd.concat(dfs, ignore_index=True)
    # Remove duplicatas exatas de transação
    if "ID Transação" in merged.columns:
        merged = merged.drop_duplicates(subset=["ID Transação"])
    return merged


def get_products(df: pd.DataFrame) -> list[str]:
    if "Nome do Produto" not in df.columns:
        return []
    return sorted(df["Nome do Produto"].dropna().unique().tolist())


def get_status_options(df: pd.DataFrame) -> list[str]:
    if "Status" not in df.columns:
        return []
    return sorted(df["Status"].dropna().unique().tolist())
