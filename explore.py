"""
Descobre o range de IDs validos para Bramil e Spani
testando IDs estrategicos no endpoint de detalhes.
"""

import requests

# ---- BRAMIL ----
BRAMIL = {
    "nome":      "BRAMIL",
    "token":     "",  # <-- cole o BRAMIL_TOKEN aqui
    "sessao_id": "",  # <-- cole o BRAMIL_SESSAO_ID aqui
    "base":      "https://services-beta.vipcommerce.com.br/api-admin/v1",
    "org":       "53",
    "filial":    "1",
    "cd":        "21",
    "domainkey": "bramilemcasa.com.br",
    "origin":    "http://localhost",
    "referer":   "http://localhost/",
    "xreq":      "br.com.bramilemcasa.appvendas",
}

# ---- SPANI ----
SPANI = {
    "nome":      "SPANI",
    "token":     "",  # <-- cole o SPANI_TOKEN aqui
    "sessao_id": "",  # <-- cole o SPANI_SESSAO_ID aqui
    "base":      "https://services-beta.vipcommerce.com.br/api-admin/v1",
    "org":       "67",
    "filial":    "1",
    "cd":        "6",
    "domainkey": "spanionline.com.br",
    "origin":    "http://localhost",
    "referer":   "http://localhost/",
    "xreq":      "br.com.spanionline.appvendas",
}


def make_headers(cfg):
    return {
        "Authorization":    f"Bearer {cfg['token']}",
        "Content-Type":     "application/json",
        "Accept":           "application/json",
        "DomainKey":        cfg["domainkey"],
        "OrganizationId":   cfg["org"],
        "sessao-id":        cfg["sessao_id"],
        "User-Agent":       "Mozilla/5.0 (Linux; Android 15; SM-A155M Build/AP3A.240905.015.A2; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/144.0.7559.132 Mobile Safari/537.36",
        "Origin":           cfg["origin"],
        "Referer":          cfg["referer"],
        "X-Requested-With": cfg["xreq"],
    }


def testar_id(cfg, pid):
    headers = make_headers(cfg)
    url = f"{cfg['base']}/org/{cfg['org']}/filial/{cfg['filial']}/centro_distribuicao/{cfg['cd']}/loja/produtos/{pid}/detalhes"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if data.get("success") and data.get("data", {}).get("produto"):
                nome = data["data"]["produto"].get("descricao", "")
                return f"200 OK ({nome[:40]})"
        return f"{r.status_code}"
    except Exception as e:
        return f"ERRO: {e}"


def descobrir_range(cfg):
    print(f"\n{'='*50}")
    print(f"  {cfg['nome']}")
    print(f"{'='*50}")

    if not cfg["token"]:
        print("  Token nao configurado - pule esta secao")
        return

    testes = [1, 100, 500, 1000, 2000, 5000, 10000, 15000, 20000, 25000, 30000]
    ultimo_ok = 0

    for pid in testes:
        resultado = testar_id(cfg, pid)
        ok = "OK" in resultado
        if ok:
            ultimo_ok = pid
        print(f"  ID {pid:6d}: {resultado}")

    print(f"\n  Ultimo ID com resposta OK: {ultimo_ok}")
    print(f"  Sugestao de ID_FIM para varredura: {int(ultimo_ok * 1.2)}")


descobrir_range(BRAMIL)
descobrir_range(SPANI)