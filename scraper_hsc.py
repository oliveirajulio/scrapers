"""
========================================
SCRAPER HSC - DELIVERY ON API
========================================
Gera: produtos_hsc.csv + alertas_hsc.txt (se houver)
"""

import os
import json
import csv
import time
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------------
# CONFIGURACAO - via variaveis de ambiente no Railway
# -------------------------------------------------------
AUTH      = os.environ.get("HSC_AUTH", "")
DATA_DIR  = os.environ.get("DATA_DIR", "/data")

BASE_URL   = "https://api-hsc.deliveryon.com.br/v2"
SUBDOMINIO = "hsc"

ALERTA_DESCONTO_MINIMO = 10
ALERTA_AUMENTO_MINIMO  = 15
ALERTA_ESTOQUE_BAIXO   = 5

CSV_PATH      = os.path.join(DATA_DIR, "produtos_hsc.csv")
ALERTA_PATH   = os.path.join(DATA_DIR, "alertas_hsc.txt")
SNAPSHOT_PATH = os.path.join(DATA_DIR, "_snapshot_hsc.json")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

HEADERS = {
    "Authorization": AUTH,
    "User-Agent":    "okhttp/2.3.0",
    "Accept":        "application/json",
}


def baixar_categorias():
    print("[1/3] Baixando categorias e produtos...\n")
    resp = requests.get(f"{BASE_URL}/GetCategoriasBySubdominio/{SUBDOMINIO}", headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    categorias = data.get("categorias", [])
    if categorias:
        print(f"  DEBUG campos: {list(categorias[0].keys())}")
    dic_cats = {c.get("id_grupo", c.get("id", "")): c.get("nome_grupo", c.get("nome", c.get("descricao", ""))) for c in categorias}
    print(f"  {len(categorias)} categorias encontradas\n")
    return dic_cats


def baixar_produtos():
    todos = []
    ids_com_produtos = 0

    for id_cat in range(1, 120):
        print(f"  [{id_cat}/119] ID {id_cat}...", end="", flush=True)
        offset, pagina, id_prods, tem_prods = 0, 1, [], False

        while True:
            url = f"{BASE_URL}/getProdutosByCategoria/{SUBDOMINIO}/{id_cat}/{offset}/0/alfabetica"
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                data = resp.json()

                if data.get("produtos"):
                    if not tem_prods:
                        print()
                        tem_prods = True
                    id_prods.extend(data["produtos"])
                    print(f"    Pagina {pagina}: {len(data['produtos'])} produtos")
                    next_offset = data.get("nextOffset")
                    if next_offset:
                        offset = next_offset
                        pagina += 1
                    else:
                        break
                else:
                    if not tem_prods:
                        print(" vazio")
                    break

                time.sleep(0.15)
            except Exception as e:
                if not tem_prods:
                    print(f" erro: {e}")
                break

        if id_prods:
            print(f"    OK - {len(id_prods)} produtos")
            todos.extend(id_prods)
            ids_com_produtos += 1

    print(f"\n  Total: {len(todos)} produtos em {ids_com_produtos} categorias\n")

    vistos, unicos = set(), []
    for p in todos:
        pid = p.get("id_produto")
        if pid not in vistos:
            vistos.add(pid)
            unicos.append(p)
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

        dic_antigos = {p["id_produto"]: p for p in produtos_antigos}
        novos = removidos = mudancas = mudancas_est = 0

        for prod_novo in produtos_novos:
            pid = prod_novo.get("id_produto")
            if pid in dic_antigos:
                prod_antigo = dic_antigos.pop(pid)
                p_novo   = float(prod_novo.get("preco_venda", 0) or 0)
                p_antigo = float(prod_antigo.get("preco_venda", 0) or 0)
                e_novo   = float(prod_novo.get("estoque_atual", 0) or 0)
                e_antigo = float(prod_antigo.get("estoque_atual", 0) or 0)

                if p_novo != p_antigo and p_antigo > 0:
                    mudancas += 1
                    variacao = ((p_novo - p_antigo) / p_antigo) * 100
                    if variacao <= -ALERTA_DESCONTO_MINIMO:
                        alertas.append({"tipo": "[DESCONTO]",      "produto": prod_novo.get("dprodweb", ""), "detalhes": f"R$ {p_antigo:.2f} -> R$ {p_novo:.2f} ({variacao:.1f}%)"})
                    elif variacao >= ALERTA_AUMENTO_MINIMO:
                        alertas.append({"tipo": "[AUMENTO]",       "produto": prod_novo.get("dprodweb", ""), "detalhes": f"R$ {p_antigo:.2f} -> R$ {p_novo:.2f} (+{variacao:.1f}%)"})

                if e_novo != e_antigo:
                    mudancas_est += 1
                    if 0 < e_novo <= ALERTA_ESTOQUE_BAIXO:
                        alertas.append({"tipo": "[ESTOQUE BAIXO]", "produto": prod_novo.get("dprodweb", ""), "detalhes": f"Apenas {int(e_novo)} unidade(s)"})
                    if e_antigo == 0 and e_novo > 0:
                        alertas.append({"tipo": "[REABASTECIDO]",  "produto": prod_novo.get("dprodweb", ""), "detalhes": f"Voltou ao estoque - {int(e_novo)} unidade(s)"})
            else:
                novos += 1

        removidos = len(dic_antigos)
        print(f"  Novos: {novos} | Removidos: {removidos} | Preco: {mudancas} | Estoque: {mudancas_est} | Alertas: {len(alertas)}\n")

    except Exception as e:
        print(f"  AVISO: Erro ao comparar: {e}\n")

    return alertas


def salvar(produtos_unicos, dic_cats, alertas):
    print("[3/3] Salvando arquivos...")
    hoje = datetime.now().strftime("%Y-%m-%d")

    campos = [
        "id_produto", "codigo_barras", "nome", "categoria", "preco", "preco_promocional",
        "preco_original", "estoque", "grupo_id", "subgrupo_id", "descricao",
        "nome_foto", "fracionada", "permite_adicionais", "dt_entrada",
    ]
    linhas = []
    for p in produtos_unicos:
        promo = p.get("valor_promocional")
        linhas.append({
            "id_produto":         p.get("id_produto", ""),
            "codigo_barras":      p.get("cproduto", ""),
            "nome":               p.get("dprodweb", ""),
            "categoria":          dic_cats.get(p.get("id_grupo"), f"ID {p.get('id_grupo')}"),
            "preco":              f"{float(p.get('preco_venda', 0) or 0):.2f}",
            "preco_promocional":  f"{float(promo):.2f}" if promo else "",
            "preco_original":     f"{float(p.get('preco_original', 0) or 0):.2f}",
            "estoque":            f"{float(p.get('estoque_atual', 0) or 0):.0f}",
            "grupo_id":           p.get("id_grupo", ""),
            "subgrupo_id":        p.get("id_subgrupo", ""),
            "descricao":          p.get("descricao", ""),
            "nome_foto":          p.get("nome_foto", ""),
            "fracionada":         p.get("fracionada", ""),
            "permite_adicionais": p.get("permite_adicionais", ""),
            "dt_entrada":         hoje,
        })

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(linhas)
    print(f"  OK -> {CSV_PATH}")

    if alertas:
        data_exibicao = datetime.now().strftime("%d/%m/%Y %H:%M")
        relatorio = f"ALERTAS HSC - {data_exibicao}\n{'=' * 44}\nTotal: {len(alertas)}\n\n"
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
    print("  SCRAPER HSC - DELIVERY ON")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=============================================\n")

    dic_cats = baixar_categorias()
    produtos  = baixar_produtos()
    alertas   = comparar_e_alertar(produtos)
    salvar(produtos, dic_cats, alertas)

    com_estoque = sum(1 for p in produtos if float(p.get("estoque_atual", 0) or 0) > 0)
    precos = [float(p.get("preco_venda", 0) or 0) for p in produtos if float(p.get("preco_venda", 0) or 0) > 0]

    print("=============================================")
    print(f"  Total: {len(produtos)} produtos ({com_estoque} em estoque)")
    if precos:
        print(f"  Preco min/max/med: R$ {min(precos):.2f} / R$ {max(precos):.2f} / R$ {sum(precos)/len(precos):.2f}")
    print(f"  Alertas: {len(alertas)}")
    print("  HSC CONCLUIDO!")
    print("=============================================\n")


if __name__ == "__main__":
    main()