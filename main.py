"""
========================================
MAIN - SCRAPER + UPLOAD
========================================
Roda em sequencia:
1. Scraper Bramil
2. Scraper HSC (Casa do Arroz)
3. Scraper Spani
4. Upload de todos os CSVs pro backend
"""

import os
import sys
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Importa os scrapers como modulos
import scraper_bramil
import scraper_hsc
import scraper_spani

# ===== CONFIGURAÇÕES DO UPLOAD =====
DATA_DIR     = os.environ.get("DATA_DIR", "/data")
URL          = os.environ.get("UPLOAD_URL", "https://pouppe-production.up.railway.app/admin/import-products")
NOTIFY_URL   = os.environ.get("NOTIFY_URL", "https://pouppe-production.up.railway.app/admin/notify-price-changes")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "")

MERCADOS = [
    {"arquivo": "produtos_bramil.csv",         "mercado": "Bramil Supermercados"},
    {"arquivo": "produtos_casa_do_arroz.csv",   "mercado": "Casa do Arroz"},
    {"arquivo": "produtos_spani.csv",           "mercado": "Spani Atacadista"},
]


def separador(titulo=""):
    print("\n" + "=" * 60)
    if titulo:
        print(f"  {titulo}")
        print("=" * 60)


def rodar_scraper(nome, modulo):
    separador(f"SCRAPER: {nome}")
    try:
        modulo.main()
        print(f"  ✅ {nome} concluido")
        return True
    except Exception as e:
        print(f"  ❌ {nome} falhou: {e}")
        import traceback
        traceback.print_exc()
        return False


def upload_mercado(arquivo, mercado):
    csv_path = os.path.join(DATA_DIR, arquivo)

    if not os.path.exists(csv_path):
        print(f"  ⚠️  Arquivo não encontrado: {csv_path} — pulando")
        return False

    file_size = os.path.getsize(csv_path) / (1024 * 1024)
    print(f"  📄 {arquivo} ({file_size:.2f} MB)")

    try:
        with open(csv_path, "rb") as f:
            files   = {"csv_file": (arquivo, f, "text/csv")}
            data    = {"market_name": mercado}
            headers = {"X-Admin-Secret": ADMIN_SECRET}
            response = requests.post(URL, headers=headers, files=files, data=data, timeout=300)

        if response.status_code == 200:
            result = response.json()
            print(f"  ✅ Importado com sucesso!")
            if "results" in result:
                for item in result["results"]:
                    print(f"     Status: {item.get('status')}")
            return True
        elif response.status_code == 401:
            print(f"  ❌ Erro de autenticação — verifique ADMIN_SECRET")
        elif response.status_code == 400:
            print(f"  ❌ Erro na requisição: {response.text}")
        elif response.status_code == 500:
            print(f"  ❌ Erro no servidor: {response.text}")
        else:
            print(f"  ⚠️  Status {response.status_code}: {response.text}")

    except requests.exceptions.Timeout:
        print(f"  ❌ Timeout")
    except requests.exceptions.ConnectionError:
        print(f"  ❌ Erro de conexão")
    except Exception as e:
        print(f"  ❌ Erro inesperado: {e}")

    return False


def enviar_notificacoes():
    separador("NOTIFICAÇÕES PUSH")
    try:
        response = requests.post(NOTIFY_URL, json={"hours": 1}, timeout=60)

        if response.status_code == 200:
            result = response.json()
            print(f"  ✅ Notificações enviadas!")
            print(f"     Total de alertas:      {result.get('total_alerts', 0)}")
            print(f"     Usuários notificados:  {result.get('users_notified', 0)}")
            print(f"     Notificações enviadas: {result.get('notifications_sent', 0)}")
        else:
            print(f"  ⚠️  Erro ao notificar (Status {response.status_code}): {response.text}")

    except requests.exceptions.Timeout:
        print(f"  ⏱️  Timeout ao enviar notificações")
    except Exception as e:
        print(f"  ❌ Erro: {e}")


def main():
    inicio = datetime.now()

    separador(f"INICIO - {inicio.strftime('%d/%m/%Y %H:%M')}")

    # ===== ETAPA 1: SCRAPERS =====
    separador("ETAPA 1/2 - SCRAPERS")

    scraper_ok = {
        "Bramil":       rodar_scraper("Bramil",       scraper_bramil),
        "Casa do Arroz": rodar_scraper("Casa do Arroz", scraper_hsc),
        "Spani":        rodar_scraper("Spani",        scraper_spani),
    }

    scrapers_ok  = sum(1 for v in scraper_ok.values() if v)
    scrapers_fail = sum(1 for v in scraper_ok.values() if not v)
    print(f"\n  Scrapers: {scrapers_ok} ok / {scrapers_fail} com falha")

    # ===== ETAPA 2: UPLOAD =====
    separador("ETAPA 2/2 - UPLOAD")

    uploads_ok   = 0
    uploads_fail = 0

    for m in MERCADOS:
        print(f"\n🏪 {m['mercado']}")
        ok = upload_mercado(m["arquivo"], m["mercado"])
        if ok:
            uploads_ok += 1
        else:
            uploads_fail += 1

    # ===== NOTIFICAÇÕES =====
    if uploads_ok > 0:
        enviar_notificacoes()
    else:
        print("\n⚠️  Nenhum upload bem sucedido — notificações não enviadas")

    # ===== RESUMO =====
    fim = datetime.now()
    duracao = (fim - inicio).seconds // 60

    separador("RESUMO FINAL")
    print(f"  ⏱️  Duração: {duracao} minutos")
    print(f"  🔍 Scrapers:  {scrapers_ok}/3 ok")
    print(f"  📤 Uploads:   {uploads_ok}/3 ok")
    print("=" * 60)


if __name__ == "__main__":
    main()