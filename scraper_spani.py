"""
========================================
SCRAPER SPANI - VIPCOMMERCE API
========================================
Gera: produtos_spani.csv + alertas_spani.txt (se houver)
Tokens expiram a cada sessao do app.
Se der erro 401/403, atualize SPANI_TOKEN e SPANI_SESSAO_ID.
"""

import os
import json
import csv
import time
import requests
from datetime import datetime
from pathlib import Path

# -------------------------------------------------------
# CONFIGURACAO - via variaveis de ambiente no Railway
# -------------------------------------------------------
TOKEN     = os.environ.get("SPANI_TOKEN", "")
SESSAO_ID = os.environ.get("SPANI_SESSAO_ID", "")
DATA_DIR  = os.environ.get("DATA_DIR", "/data")

BASE_URL = "https://services-beta.vipcommerce.com.br/api-admin/v1"
ORG      = "67"
FILIAL   = "1"
CD       = "6"

ALERTA_DESCONTO_MINIMO = 10
ALERTA_AUMENTO_MINIMO  = 15

CSV_PATH      = os.path.join(DATA_DIR, "produtos_spani.csv")
ALERTA_PATH   = os.path.join(DATA_DIR, "alertas_spani.txt")
SNAPSHOT_PATH = os.path.join(DATA_DIR, "_snapshot_spani.json")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Authorization":    f"Bearer {TOKEN}",
    "Content-Type":     "application/json",
    "Accept":           "application/json",
    "DomainKey":        "spanionline.com.br",
    "OrganizationId":   ORG,
    "sessao-id":        SESSAO_ID,
    "User-Agent":       "Mozilla/5.0 (Linux; Android 15; SM-A155M Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.132 Mobile Safari/537.36",
    "Origin":           "http://localhost",
    "Referer":          "http://localhost/",
    "X-Requested-With": "br.com.spanionline.appvendas",
}

DEPARTAMENTOS = [
    {"id": 1,  "descricao": "Pet"},
    {"id": 2,  "descricao": "Bazar e Utilidades"},
    {"id": 3,  "descricao": "Bebidas"},
    {"id": 4,  "descricao": "Biscoitos"},
    {"id": 5,  "descricao": "Carnes"},
    {"id": 6,  "descricao": "Cereais"},
    {"id": 7,  "descricao": "Congelados"},
    {"id": 8,  "descricao": "Frios e Laticinios"},
    {"id": 9,  "descricao": "Hortifruit"},
    {"id": 10, "descricao": "Limpeza"},
    {"id": 11, "descricao": "Matinais"},
    {"id": 12, "descricao": "Mercearia"},
    {"id": 13, "descricao": "Padaria"},
    {"id": 14, "descricao": "Perfumaria e Higiene"},
]

GRUPO_MAP = {d["id"]: str(d["id"]) for d in DEPARTAMENTOS}


def preco_efetivo(p):
    if p.get("em_oferta") and p.get("oferta"):
        return float(p["oferta"].get("preco_oferta", 0) or 0)
    return float(p.get("preco", 0) or 0)


def preco_original(p):
    if p.get("em_oferta") and p.get("oferta"):
        return float(p["oferta"].get("preco_antigo", 0) or 0)
    return float(p.get("preco", 0) or 0)


def baixar_produtos():
    print("[1/3] Baixando produtos...\n")
    todos = []

    for dep in DEPARTAMENTOS:
        dep_id, dep_nome = dep["id"], dep["descricao"]
        pagina, total_paginas = 1, 1
        dep_produtos = []

        print(f"  {dep_nome} (ID {dep_id})")

        while pagina <= total_paginas:
            url = (
                f"{BASE_URL}/org/{ORG}/filial/{FILIAL}/centro_distribuicao/{CD}"
                f"/loja/classificacoes_mercadologicas/departamentos/{dep_id}/produtos?page={pagina}"
            )
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                if data.get("success") and data.get("data"):
                    dep_produtos.extend(data["data"])
                    if pagina == 1 and data.get("paginator"):
                        total_paginas = data["paginator"].get("total_pages", 1)
                    print(f"    Pagina {pagina}/{total_paginas} - {len(data['data'])} produtos")
                    pagina += 1
                else:
                    break

                time.sleep(0.2)
            except Exception as e:
                print(f"    ERRO pagina {pagina}: {e}")
                break

        for p in dep_produtos:
            p["departamento_id"]   = dep_id
            p["departamento_nome"] = dep_nome

        print(f"    -> {len(dep_produtos)} produtos\n")
        todos.extend(dep_produtos)
        time.sleep(0.3)

    vistos, unicos = set(), []
    for p in todos:
        pid = p.get("produto_id")
        if pid not in vistos:
            vistos.add(pid)
            unicos.append(p)

    print(f"  Total: {len(unicos)} produtos unicos\n")
    return unicos


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


def salvar(produtos_unicos, alertas):
    print("[3/3] Salvando arquivos...")
    hoje = datetime.now().strftime("%Y-%m-%d")

    campos = [
        "id_produto", "codigo_barras", "nome", "preco", "preco_promocional",
        "preco_original", "estoque", "grupo_id", "subgrupo_id", "descricao",
        "nome_foto", "fracionada", "permite_adicionais", "dt_entrada",
        "departamento", "em_oferta", "disponivel", "sku",
    ]
    linhas = []
    for p in produtos_unicos:
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
            "grupo_id":           GRUPO_MAP.get(p.get("departamento_id"), "0"),
            "subgrupo_id":        "",
            "descricao":          "",
            "nome_foto":          p.get("imagem", ""),
            "fracionada":         "1" if p.get("unidade_sigla") == "KG" else "0",
            "permite_adicionais": "N",
            "dt_entrada":         hoje,
            "departamento":       p.get("departamento_nome", ""),
            "em_oferta":          str(p.get("em_oferta", "")),
            "disponivel":         str(p.get("disponivel", "")),
            "sku":                p.get("sku", ""),
        })

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(linhas)
    print(f"  OK -> {CSV_PATH}")

    if alertas:
        data_exibicao = datetime.now().strftime("%d/%m/%Y %H:%M")
        relatorio = f"ALERTAS SPANI - {data_exibicao}\n{'=' * 44}\nTotal: {len(alertas)}\n\n"
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
        json.dump(produtos_unicos, f, ensure_ascii=False)
    print(f"  OK -> snapshot salvo\n")


def main():
    print("=============================================")
    print("  SCRAPER SPANI - VIPCOMMERCE")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=============================================\n")

    produtos = baixar_produtos()
    alertas  = comparar_e_alertar(produtos)
    salvar(produtos, alertas)

    disponiveis = sum(1 for p in produtos if p.get("disponivel"))
    em_oferta   = sum(1 for p in produtos if p.get("em_oferta"))
    precos = [preco_efetivo(p) for p in produtos if preco_efetivo(p) > 0]

    print("=============================================")
    print(f"  Total: {len(produtos)} produtos")
    print(f"  Disponiveis: {disponiveis} | Em oferta: {em_oferta}")
    if precos:
        print(f"  Preco min/max/med: R$ {min(precos):.2f} / R$ {max(precos):.2f} / R$ {sum(precos)/len(precos):.2f}")
    print(f"  Alertas: {len(alertas)}")
    print("  SPANI CONCLUIDO!")
    print("=============================================\n")


if __name__ == "__main__":
    main()