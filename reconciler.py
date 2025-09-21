#!/usr/bin/env python3
"""
Command‑line utility to reconcile a credit card statement against a Mobills export.

Usage:
    python reconciler.py --fatura caminho_da_fatura.csv --mobills caminho_do_mobills.xlsx

The script reads the provided files (CSV or Excel), asks the user to specify
which columns represent the date and the transaction value, and then
identifies any discrepancies between the two datasets.  It writes two
CSV files – one for entries present in the statement but not in Mobills,
and another for entries present in Mobills but absent from the statement –
and prints a summary of the totals to standard output.

Only matches on exact date and value combinations are considered
reconciled.  Duplicate values on the same day are matched one by one.
"""

import argparse
import pandas as pd
import numpy as np
import re
import sys
from typing import Tuple


def normalise_numeric(value) -> float | None:
    """Convert a variety of numeric formats to a float.

    Handles Brazilian formats where the thousand separator is a dot and the
    decimal separator is a comma, as well as standard international formats.
    Returns None if the value cannot be interpreted as a number.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    s = str(value).strip()
    s = re.sub(r"[Rr\$\s]", "", s)
    if "," in s and s.count(",") == 1:
        s = s.replace(".", "").replace(",", ".")
    if s.count(",") > 1:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def reconcile(
    df_fatura: pd.DataFrame,
    df_mobills: pd.DataFrame,
    date_col_f: str,
    value_col_f: str,
    date_col_m: str,
    value_col_m: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, float, float, float]:
    """Perform reconciliation on two datasets.

    Returns the unmatched rows and the totals from each dataset and their difference.
    """
    f = df_fatura.copy().reset_index(drop=True)
    m = df_mobills.copy().reset_index(drop=True)
    f["_date"] = pd.to_datetime(f[date_col_f], errors="coerce", dayfirst=True)
    m["_date"] = pd.to_datetime(m[date_col_m], errors="coerce", dayfirst=True)
    f["_value"] = f[value_col_f].apply(normalise_numeric)
    m["_value"] = m[value_col_m].apply(normalise_numeric)
    f_valid = f.dropna(subset=["_date", "_value"]).copy()
    m_valid = m.dropna(subset=["_date", "_value"]).copy()
    f_valid["_matched"] = False
    m_valid["_matched"] = False
    for idx_f, row_f in f_valid.iterrows():
        if row_f["_matched"]:
            continue
        candidates = m_valid[(m_valid["_matched"] == False) & (m_valid["_date"] == row_f["_date"]) & (m_valid["_value"] == row_f["_value"])]
        if len(candidates) > 0:
            idx_m = candidates.index[0]
            f_valid.at[idx_f, "_matched"] = True
            m_valid.at[idx_m, "_matched"] = True
    unmatched_f = f_valid[~f_valid["_matched"]]
    unmatched_m = m_valid[~m_valid["_matched"]]
    total_f = f_valid["_value"].sum()
    total_m = m_valid["_value"].sum()
    difference = total_f - total_m
    return unmatched_f, unmatched_m, total_f, total_m, difference


def choose_columns(df: pd.DataFrame, role: str) -> Tuple[str, str]:
    """Ask the user to select the date and value columns via stdin.

    Prints column names and returns the names chosen by the user.
    """
    print(f"\nColunas disponíveis no arquivo {role}:")
    for i, col in enumerate(df.columns):
        print(f"  [{i}] {col}")
    idx_date = input(f"Selecione o índice da coluna que representa a **Data** no {role}: ")
    idx_value = input(f"Selecione o índice da coluna que representa o **Valor** no {role}: ")
    try:
        date_col = df.columns[int(idx_date)]
        value_col = df.columns[int(idx_value)]
    except Exception:
        print("Entrada inválida. Abortando.")
        sys.exit(1)
    return date_col, value_col


def load_file(path: str) -> pd.DataFrame:
    """Load CSV or Excel file into a DataFrame."""
    ext = path.lower().split(".")[-1]
    if ext == "csv":
        # Try encodings common in Brazil
        for encoding in ["utf-8", "latin-1"]:
            try:
                return pd.read_csv(path, encoding=encoding)
            except Exception:
                continue
        return pd.read_csv(path)
    else:
        return pd.read_excel(path)


def main():
    parser = argparse.ArgumentParser(description="Concilia fatura de cartão com lançamentos do Mobills.")
    parser.add_argument("--fatura", required=True, help="Caminho para o arquivo da fatura (CSV ou XLS/XLSX)")
    parser.add_argument("--mobills", required=True, help="Caminho para o arquivo exportado do Mobills (CSV ou XLS/XLSX)")
    parser.add_argument("--outdir", default=".", help="Diretório onde serão salvos os relatórios de inconsistência")
    args = parser.parse_args()

    try:
        df_fatura = load_file(args.fatura)
    except Exception as e:
        print(f"Erro ao ler a fatura: {e}")
        sys.exit(1)
    try:
        df_mobills = load_file(args.mobills)
    except Exception as e:
        print(f"Erro ao ler o Mobills: {e}")
        sys.exit(1)

    date_col_f, value_col_f = choose_columns(df_fatura, "fatura")
    date_col_m, value_col_m = choose_columns(df_mobills, "Mobills")

    unmatched_f, unmatched_m, total_f, total_m, difference = reconcile(
        df_fatura, df_mobills, date_col_f, value_col_f, date_col_m, value_col_m
    )

    # Save results
    f_out = f"{args.outdir}/fatura_nao_conciliada.csv"
    m_out = f"{args.outdir}/mobills_nao_conciliado.csv"
    unmatched_f.to_csv(f_out, index=False)
    unmatched_m.to_csv(m_out, index=False)
    print("\n--- RESUMO DA CONCILIAÇÃO ---")
    print(f"Total da fatura: R$ {total_f:,.2f}")
    print(f"Total do Mobills: R$ {total_m:,.2f}")
    print(f"Diferença (fatura - Mobills): R$ {difference:,.2f}")
    print("\nRelatórios de inconsistências salvos em:")
    print(f"  - {f_out}")
    print(f"  - {m_out}")


if __name__ == "__main__":
    main()
