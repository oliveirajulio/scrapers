"""
========================================
SCRAPER ROYAL SUPERMERCADOS - VIPCOMMERCE API
========================================
Gera: produtos_royal.csv + alertas_royal.txt (se houver)

Variaveis de ambiente (.env ou Railway):
  ROYAL_TOKEN     = Bearer token
  ROYAL_SESSAO_ID = sessao-id
  DATA_DIR        = /data (opcional, padrao ./)
"""

import os
import json
import csv
import time
import requests
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# -------------------------------------------------------
# CONFIGURACAO
# -------------------------------------------------------
TOKEN     = os.environ.get("ROYAL_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJ2aXBjb21tZXJjZSIsImF1ZCI6ImFwaS1hZG1pbiIsInN1YiI6IjZiYzQ4NjdlLWRjYTktMTFlOS04NzQyLTAyMGQ3OTM1OWNhMCIsInZpcGNvbW1lcmNlQ2xpZW50ZUlkIjoiY2RkNjAyYTQtMTFhYi0xMWYxLTlkZWUtZmExNjNlODQxNjM4IiwiaWF0IjoxNzcyMTQxMzI0LCJ2ZXIiOjEsImNsaWVudCI6OTU1OTUsIm9wZXJhdG9yIjpudWxsLCJvcmciOiIyNTUifQ.hlUwTYRMqD5pLMpiihdK_h8N9X1ganKe72atXA7_9b46S3-Bi_l5qH8mwql9qC0Th-kASTm3nT5AMukxoTQtZQ")
SESSAO_ID = os.environ.get("ROYAL_SESSAO_ID", "1c43d44a0317b39331780a02eb17482a")
DATA_DIR  = os.environ.get("DATA_DIR", "")

BASE_URL = "https://services.vipcommerce.com.br/api-admin/v1"
ORG      = "255"
FILIAL   = "2"
CD       = "1"

# Range de IDs a varrer
ID_INICIO = 1
ID_FIM    = 22000
WORKERS   = 20  # threads paralelas (reduza para 10 se der erro 429)

ALERTA_DESCONTO_MINIMO = 10
ALERTA_AUMENTO_MINIMO  = 15

CSV_PATH      = os.path.join(DATA_DIR, "produtos_royal.csv")
ALERTA_PATH   = os.path.join(DATA_DIR, "alertas_royal.txt")
SNAPSHOT_PATH = os.path.join(DATA_DIR, "_snapshot_royal.json")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Authorization":  f"Bearer {TOKEN}",
    "Content-Type":   "application/json",
    "Accept":         "application/json",
    "domainkey":      "royaleemporio.com.br",
    "organizationid": ORG,
    "sessao-id":      SESSAO_ID,
    "User-Agent":     "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 CrKey/1.54.250320",
    "Origin":         "https://www.royaleemporio.com.br",
    "Referer":        "https://www.royaleemporio.com.br/",
}

GRUPO_MAP = {
    5: "11", 14: "97", 13: "82", 6: "83", 8: "24",
    12: "51", 16: "50", 3: "52", 9: "21", 4: "53",
    10: "22", 15: "84", 7: "23", 11: "66",
}


def preco_efetivo(p):
    if p.get("em_oferta") and p.get("oferta"):
        return float(p["oferta"].get("preco_oferta", 0) or 0)
    return float(p.get("preco", 0) or 0)


def preco_original(p):
    if p.get("em_oferta") and p.get("oferta"):
        return float(p["oferta"].get("preco_antigo", 0) or 0)
    return float(p.get("preco", 0) or 0)


def buscar_produto(pid):
    url = f"{BASE_URL}/org/{ORG}/filial/{FILIAL}/centro_distribuicao/{CD}/loja/produtos/{pid}/detalhes"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            data = r.json()
            if data.get("success") and data.get("data", {}).get("produto"):
                return data["data"]["produto"]
        return None
    except Exception:
        return None


def baixar_produtos():
    print("[1/3] Baixando produtos por varredura de IDs...\n")

    ids = list(range(ID_INICIO, ID_FIM + 1))
    total = len(ids)
    encontrados = []
    lock = Lock()
    contador = {"ok": 0, "done": 0}
    inicio = time.time()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(buscar_produto, pid): pid for pid in ids}

        for future in as_completed(futures):
            produto = future.result()
            with lock:
                contador["done"] += 1
                if produto:
                    contador["ok"] += 1
                    encontrados.append(produto)

                if contador["done"] % 1000 == 0:
                    elapsed = time.time() - inicio
                    pct = contador["done"] / total * 100
                    rps = contador["done"] / elapsed if elapsed > 0 else 0
                    eta = (total - contador["done"]) / rps if rps > 0 else 0
                    print(f"  [{pct:.1f}%] {contador['done']}/{total} | encontrados: {contador['ok']} | {rps:.1f} req/s | ETA: {eta/60:.1f}min")

    elapsed = time.time() - inicio
    print(f"\n  Concluido em {elapsed/60:.1f} min")
    print(f"  Total produtos encontrados: {len(encontrados)}\n")
    return encontrados


def comparar_e_alertar(produtos_novos):
    print("[2/3] Comparando com execucao anterior...")
    alertas = []

    if not os.path.exists(SNAPSHOT_PATH):
        print("  Primeira execucao - sem comparacao\n")
        return alertas

    try:
        with open(SNAPSHOT_PATH, "r", encoding="utf-8") as f:
            produtos_antigos = json.load(f)

        dic_antigos = {p["produto_id"]: p for p in produtos_antigos}
        novos = removidos = mudancas = 0

        for prod_novo in produtos_novos:
            pid = prod_novo.get("produto_id")
            if pid in dic_antigos:
                prod_antigo = dic_antigos.pop(pid)
                p_novo   = preco_efetivo(prod_novo)
                p_antigo = preco_efetivo(prod_antigo)

                if p_novo != p_antigo and p_antigo > 0:
                    mudancas += 1
                    variacao = ((p_novo - p_antigo) / p_antigo) * 100
                    if variacao <= -ALERTA_DESCONTO_MINIMO:
                        alertas.append({"tipo": "[DESCONTO]", "produto": prod_novo.get("descricao", ""), "detalhes": f"R$ {p_antigo:.2f} -> R$ {p_novo:.2f} ({variacao:.1f}%)"})
                    elif variacao >= ALERTA_AUMENTO_MINIMO:
                        alertas.append({"tipo": "[AUMENTO]",  "produto": prod_novo.get("descricao", ""), "detalhes": f"R$ {p_antigo:.2f} -> R$ {p_novo:.2f} (+{variacao:.1f}%)"})
            else:
                novos += 1

        removidos = len(dic_antigos)
        print(f"  Novos: {novos} | Removidos: {removidos} | Mudancas preco: {mudancas} | Alertas: {len(alertas)}\n")

    except Exception as e:
        print(f"  AVISO: Erro ao comparar: {e}\n")

    return alertas


def salvar(produtos, alertas):
    print("[3/3] Salvando arquivos...")
    hoje = datetime.now().strftime("%Y-%m-%d")

    campos = [
        "id_produto", "codigo_barras", "nome", "preco", "preco_promocional",
        "preco_original", "estoque", "grupo_id", "subgrupo_id", "descricao",
        "nome_foto", "fracionada", "permite_adicionais", "dt_entrada",
    ]

    linhas = []
    for p in produtos:
        pe = preco_efetivo(p)
        po = preco_original(p)
        linhas.append({
            "id_produto":         p.get("produto_id", ""),
            "codigo_barras":      p.get("codigo_barras", ""),
            "nome":               p.get("descricao", ""),
            "preco":              f"{pe:.2f}",
            "preco_promocional":  f"{pe:.2f}" if p.get("em_oferta") and p.get("oferta") else "",
            "preco_original":     f"{po:.2f}",
            "estoque":            "0",
            "grupo_id":           "0",
            "subgrupo_id":        "",
            "descricao":          "",
            "nome_foto":          p.get("imagem", ""),
            "fracionada":         "1" if p.get("unidade_sigla") == "KG" else "0",
            "permite_adicionais": "N",
            "dt_entrada":         hoje,
        })

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(linhas)
    print(f"  OK -> {CSV_PATH} ({len(linhas)} produtos)")

    if alertas:
        data_exibicao = datetime.now().strftime("%d/%m/%Y %H:%M")
        relatorio = f"ALERTAS ROYAL - {data_exibicao}\n{'=' * 44}\nTotal: {len(alertas)}\n\n"
        for a in alertas:
            relatorio += f"{a['tipo']}\nProduto: {a['produto']}\n{a['detalhes']}\n\n"
        with open(ALERTA_PATH, "w", encoding="utf-8") as f:
            f.write(relatorio)
        print(f"  OK -> {ALERTA_PATH} ({len(alertas)} alertas)")
    else:
        if os.path.exists(ALERTA_PATH):
            os.remove(ALERTA_PATH)
        print("  Sem alertas nesta execucao")

    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(produtos, f, ensure_ascii=False)
    print(f"  OK -> snapshot salvo\n")


def main():
    if not TOKEN:
        print("ERRO: ROYAL_TOKEN nao definido.")
        return
    if not SESSAO_ID:
        print("ERRO: ROYAL_SESSAO_ID nao definido.")
        return

    print("=============================================")
    print("  SCRAPER ROYAL SUPERMERCADOS - VIPCOMMERCE")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print(f"  Varrendo IDs {ID_INICIO} a {ID_FIM} ({WORKERS} workers)")
    print("=============================================\n")

    produtos = baixar_produtos()
    alertas  = comparar_e_alertar(produtos)
    salvar(produtos, alertas)

    precos = [preco_efetivo(p) for p in produtos if preco_efetivo(p) > 0]
    print("=============================================")
    print(f"  Total: {len(produtos)} produtos")
    if precos:
        print(f"  Preco min/max/med: R$ {min(precos):.2f} / R$ {max(precos):.2f} / R$ {sum(precos)/len(precos):.2f}")
    print(f"  Alertas: {len(alertas)}")
    print("  ROYAL CONCLUIDO!")
    print("=============================================\n")


if __name__ == "__main__":
    main()