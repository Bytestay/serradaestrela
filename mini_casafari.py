
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mini_casafari.py — suporta ficheiro de configuração YAML (--config)
Perfis: podes ter config_light.yml e config_full.yml sem tocar no workflow.
"""
import re, sys, math, time, argparse, glob, os, random, collections, datetime, json
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlencode
import requests
from bs4 import BeautifulSoup
import pandas as pd

try:
    import yaml
except Exception as e:
    print("PyYAML é necessário. Instala com: pip install pyyaml", file=sys.stderr)
    raise

DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}

@dataclass
class Listing:
    source: str
    title: str
    price_eur: float
    url: str
    location: str
    typology: str
    details: str
    image_url: str = ""

def parse_price_to_float(text: str) -> float:
    if not text: return math.nan
    nums = re.sub(r'[^0-9]', '', text)
    return float(nums) if nums else math.nan

def any_keyword(text: str, keywords):
    t = (text or "").lower()
    return any(k.lower() in t for k in keywords) if keywords else True

def locality_match(text: str, localities):
    if not localities: return True
    t = (text or "").lower()
    return any(loc.lower() in t for loc in localities)

def read_localities(localities_list, localities_file):
    out = []
    if localities_file and os.path.exists(localities_file):
        with open(localities_file, "r", encoding="utf-8") as f:
            for line in f:
                v = line.strip()
                if v and not v.startswith("#"):
                    out.append(v)
    if out:
        return out
    return localities_list or []

def parse_per_source_limit_map(m):
    # accepts dict or "k=v,k=v" string
    if isinstance(m, dict):
        return {str(k).lower(): int(v) for k,v in m.items()}
    if isinstance(m, str):
        out = {}
        for part in m.split(","):
            part = part.strip()
            if not part: continue
            if "=" in part:
                k,v = part.split("=",1)
                try: out[k.strip().lower()] = int(v.strip())
                except: pass
        return out
    return {}

def http_get(session, url, timeout=25, retries=3, backoff_base=0.8):
    """GET with simple retry + exponential backoff + jitter"""
    last = None
    for attempt in range(1, retries+1):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code >= 200 and r.status_code < 400 and r.text:
                return r
            last = r
        except Exception as e:
            last = e
        sleep = backoff_base * (2 ** (attempt-1)) + random.uniform(0, 0.3)
        time.sleep(sleep)
    if isinstance(last, requests.Response):
        return last
    raise RuntimeError(f"GET failed after {retries} attempts: {url} ({last})")

def is_block_signal(response_text: str, status_code: int):
    if status_code in (403, 429):
        return True
    t = (response_text or "").lower()
    if "captcha" in t or "are you a robot" in t or "complete the security check" in t:
        return True
    return False

def try_get_image(url: str, session, timeout, retries):
    if not url: return ""
    try:
        r = http_get(session, url, timeout=timeout, retries=retries)
        if r.status_code != 200:
            return ""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")
        og = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name":"og:image"})
        if og and og.get("content"): return og["content"]
        img = soup.find("img")
        if img and img.get("src"):
            src = img["src"]
            if src.startswith("//"): src = "https:" + src
            if src.startswith("http"): return src
    except Exception:
        return ""
    return ""

# ---- Scrapers (best‑effort; podem precisar de ajuste nos seletores) ----
def scrape_idealista(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base_url = "https://www.idealista.pt"
    path = "/comprar-casas/covilha/"
    if max_price: path += f"com-preco-max_{int(max_price)}/"
    path += "tipo-moradia/"
    if min_bedrooms and min_bedrooms >= 2: path += f"t{int(min_bedrooms)}/"
    url = urljoin(base_url, path)
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    collected, page = 0, 1
    while collected < limit and page <= 5:
        pg_url = url if page == 1 else urljoin(url, f"pag-{page}.htm")
        r = http_get(s, pg_url, timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code): break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select(".item-info-container") or soup.select("article.item")
        if not cards: break
        for card in cards:
            title_el = card.select_one(".item-link") or card.select_one("a")
            price_el = card.select_one(".item-price") or card.select_one(".price")
            loc_el = card.select_one(".item-location") or card.select_one(".item-detail-location")
            desc_el = card.select_one(".item-description") or card.select_one(".item_detail")
            url_path = title_el.get("href") if title_el else None
            url_full = urljoin(base_url, url_path) if url_path else ""
            title = (title_el.get_text(strip=True) if title_el else "") or ""
            price_txt = price_el.get_text(strip=True) if price_el else ""
            price = parse_price_to_float(price_txt)
            location = (loc_el.get_text(strip=True) if loc_el else "") or ""
            details = (desc_el.get_text(strip=True) if desc_el else "") or ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("idealista", title, price, url_full, location, "", details, img))
            collected += 1
            if collected >= limit: break
        page += 1
    return items

def scrape_imovirtual(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base = "https://www.imovirtual.com/pt/"
    search_url = base + "comprar/moradia/covilha/?"
    qs = {"price_to": int(max_price) if max_price else "", "roomsNumber_from": int(min_bedrooms) if min_bedrooms else "", "page": 1}
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    while len(items) < limit and qs["page"] <= 5:
        r = http_get(s, search_url + urlencode(qs), timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code): break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("article[data-cy='listing-item']") or soup.select("article")
        if not cards: break
        for c in cards:
            a = c.select_one("a"); url_full = urljoin(base, a.get("href")) if a else ""
            title_el = c.select_one("[data-cy='listing-title']") or c.select_one("h2") or a
            title = title_el.get_text(strip=True) if title_el else ""
            price_el = c.select_one("[data-cy='listing-price']") or c.select_one(".price")
            price = parse_price_to_float(price_el.get_text(strip=True) if price_el else "")
            location_el = c.select_one("[data-cy='listing-location']") or c.select_one(".location")
            location = location_el.get_text(strip=True) if location_el else ""
            detail_el = c.select_one("[data-cy='listing-description']") or c.select_one("p")
            details = detail_el.get_text(strip=True) if detail_el else ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("imovirtual", title, price, url_full, location, "", details, img))
            if len(items) >= limit: break
        qs["page"] += 1
    return items

def scrape_casasapo(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base = "https://www.casa.sapo.pt"
    search_url = f"{base}/Casas-para-Venda/?"
    qs = {"site": "1","q": "Covilhã","tt": "1","or": "1","pvmax": int(max_price) if max_price else "","pn": 1}
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    while len(items) < limit and qs["pn"] <= 5:
        r = http_get(s, search_url + urlencode(qs), timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code): break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("div.ListItem") or soup.select("div.SearchResultProperty")
        if not cards: break
        for c in cards:
            a = c.select_one("a"); url_full = urljoin(base, a.get("href")) if a else ""
            title = a.get_text(strip=True) if a else ""
            price_el = c.select_one(".Price") or c.select_one(".price")
            price = parse_price_to_float(price_el.get_text(strip=True) if price_el else "")
            location_el = c.select_one(".LocationName") or c.select_one(".Location")
            location = location_el.get_text(strip=True) if location_el else ""
            details_el = c.select_one(".Description") or c.select_one("p")
            details = details_el.get_text(strip=True) if details_el else ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("casasapo", title, price, url_full, location, "", details, img))
            if len(items) >= limit: break
        qs["pn"] += 1
    return items

def scrape_olx(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base = "https://www.olx.pt"
    search_url = base + "/imoveis/casas-venda/covilha/?"
    qs = {"search%5Bfilter_float_price%3Ato%5D": int(max_price) if max_price else "", "page": 1}
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    while len(items) < limit and qs["page"] <= 5:
        r = http_get(s, search_url + urlencode(qs), timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code): break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("div.css-1sw7q4x") or soup.select("div.css-1apmciz")
        if not cards: break
        for c in cards:
            a = c.select_one("a"); url_full = urljoin(base, a.get("href")) if a else ""
            title = a.get_text(strip=True) if a else ""
            price_el = c.select_one("p[data-testid='ad-price']") or c.select_one("h6")
            price = parse_price_to_float(price_el.get_text(strip=True) if price_el else "")
            location_el = c.select_one("p[data-testid='location-date']") or c.select_one("p")
            location = location_el.get_text(strip=True) if location_el else ""
            details = ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("olx", title, price, url_full, location, "", details, img))
            if len(items) >= limit: break
        qs["page"] += 1
    return items

def scrape_trovit(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base = "https://casa.trovit.pt"
    search_url = base + "/index.php/cod.search_homes/type.1/what_d.covilha/price.max_{}/rooms.min_{}/".format(
        int(max_price) if max_price else 60000, int(min_bedrooms) if min_bedrooms else 2
    )
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    collected, page = 0, 1
    while collected < limit and page <= 3:
        url = search_url + f"start.{(page-1)*25}"
        r = http_get(s, url, timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code): break
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("div.item-info") or soup.select("div.item")
        if not cards: break
        for c in cards:
            a = c.select_one("a"); url_full = urljoin(base, a.get("href")) if a else ""
            title = a.get_text(strip=True) if a else ""
            price_el = c.select_one(".price") or c.select_one(".item-price")
            price = parse_price_to_float(price_el.get_text(strip=True) if price_el else "")
            location_el = c.select_one(".city") or c.select_one(".specs")
            location = location_el.get_text(strip=True) if location_el else ""
            details_el = c.select_one("p") or c.select_one(".description")
            details = details_el.get_text(strip=True) if details_el else ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("trovit", title, price, url_full, location, "", details, img))
            collected += 1
            if collected >= limit: break
        page += 1
    return items

def scrape_remax(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base = "https://www.remax.pt"
    search_url = f"{base}/comprar?search=covilha&maxprice={int(max_price) if max_price else ''}"
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    try:
        r = http_get(s, search_url, timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code):
            return items
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("div.property") or soup.select("div.card") or soup.select("article")
        for c in cards[:limit]:
            a = c.select_one("a")
            url_full = urljoin(base, a.get("href")) if a else ""
            title = a.get_text(strip=True) if a else "Imóvel Remax"
            price_el = c.select_one(".property-price") or c.select_one(".price")
            price = parse_price_to_float(price_el.get_text(strip=True) if price_el else "")
            loc_el = c.select_one(".property-location") or c.select_one(".location")
            location = loc_el.get_text(strip=True) if loc_el else ""
            det_el = c.select_one(".property-description") or c.select_one("p")
            details = det_el.get_text(strip=True) if det_el else ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("remax", title, price, url_full, location, "", details, img))
    except Exception as e:
        print(f"[WARN] remax failed: {e}", file=sys.stderr)
    return items

def scrape_era(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base = "https://www.era.pt"
    search_url = f"{base}/comprar?location=covilha&priceTo={int(max_price) if max_price else ''}"
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    try:
        r = http_get(s, search_url, timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code):
            return items
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("div.property") or soup.select("div.card") or soup.select("article")
        for c in cards[:limit]:
            a = c.select_one("a")
            url_full = urljoin(base, a.get("href")) if a else ""
            title = a.get_text(strip=True) if a else "Imóvel ERA"
            price_el = c.select_one(".price") or c.select_one(".property-price")
            price = parse_price_to_float(price_el.get_text(strip=True) if price_el else "")
            loc_el = c.select_one(".property-location") or c.select_one(".location")
            location = loc_el.get_text(strip=True) if loc_el else ""
            det_el = c.select_one(".property-description") or c.select_one("p")
            details = det_el.get_text(strip=True) if det_el else ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("era", title, price, url_full, location, "", details, img))
    except Exception as e:
        print(f"[WARN] era failed: {e}", file=sys.stderr)
    return items

def scrape_century21(max_price, min_price, min_bedrooms, localities, keywords_any, limit, timeout, retries):
    items = []
    base = "https://www.century21.pt"
    search_url = f"{base}/comprar?search=covilha&maxPrice={int(max_price) if max_price else ''}"
    s = requests.Session(); s.headers.update(DEFAULT_HEADERS)
    try:
        r = http_get(s, search_url, timeout=timeout, retries=retries)
        if r.status_code != 200 or is_block_signal(r.text, r.status_code):
            return items
        soup = BeautifulSoup(r.text, "html.parser")
        cards = soup.select("div.property") or soup.select("div.card") or soup.select("article")
        for c in cards[:limit]:
            a = c.select_one("a")
            url_full = urljoin(base, a.get("href")) if a else ""
            title = a.get_text(strip=True) if a else "Imóvel Century21"
            price_el = c.select_one(".price") or c.select_one(".property-price")
            price = parse_price_to_float(price_el.get_text(strip=True) if price_el else "")
            loc_el = c.select_one(".property-location") or c.select_one(".location")
            location = loc_el.get_text(strip=True) if loc_el else ""
            det_el = c.select_one(".property-description") or c.select_one("p")
            details = det_el.get_text(strip=True) if det_el else ""
            if not math.isnan(price):
                if min_price and price < min_price: continue
                if max_price and price > max_price: continue
            if not locality_match(f"{title} {location}", localities): continue
            if keywords_any and not any_keyword(f"{title} {details}", keywords_any): continue
            img = try_get_image(url_full, s, timeout, retries)
            items.append(Listing("century21", title, price, url_full, location, "", details, img))
    except Exception as e:
        print(f"[WARN] century21 failed: {e}", file=sys.stderr)
    return items

SCRAPERS = {
    "idealista": scrape_idealista,
    "imovirtual": scrape_imovirtual,
    "casasapo": scrape_casasapo,
    "remax": scrape_remax,
    "era": scrape_era,
    "century21": scrape_century21,
    "olx": scrape_olx,
    "trovit": scrape_trovit,
}

def write_html(df, out_html_path, title="Imóveis ≤ 60k — Covilhã & Serra da Estrela"):
    table_html = df.to_html(index=False, classes="dataframe", escape=False)
    html = f"""<!doctype html><html lang="pt"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
body {{ font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; margin:2rem; }}
h1 {{ font-size:1.6rem; margin-bottom:.25rem; }}
p.meta {{ color:#555; margin-top:0; }}
.badge {{ display:inline-block; padding:2px 6px; border-radius:4px; font-size:.8rem; }}
.badge.up {{ background:#e6ffed; border:1px solid #34d058; color:#22863a; }}
.badge.down {{ background:#ffeef0; border:1px solid #d73a49; color:#86181d; }}
.dataframe img {{ max-width: 140px; height: auto; display:block; }}
table.dataframe {{ border-collapse: collapse; width: 100%; }}
table.dataframe th, table.dataframe td {{ border:1px solid #ddd; padding:8px; vertical-align:top; }}
table.dataframe th {{ cursor:pointer; background:#f3f3f3; }}
tr:nth-child(even) {{ background:#fafafa; }}
.small {{ font-size:.9rem; }}
</style>
</head><body>
<h1>{title}</h1>
<p class="meta small">Atualizado em: <span id="ts"></span></p>
{table_html}
<script>
document.getElementById('ts').textContent = new Date().toLocaleString('pt-PT');
function sortTable(table, col, reverse) {{
  var tb = table.tBodies[0], tr = Array.prototype.slice.call(tb.rows, 0), i;
  reverse = -((+reverse) || -1);
  tr = tr.sort(function(a,b){{return reverse*(a.cells[col].textContent.trim().localeCompare(b.cells[col].textContent.trim(),'pt',{{numeric:true}}));}});
  for(i=0;i<tr.length;++i) tb.appendChild(tr[i]);
}}
(function(){{var t=document.getElementsByClassName('dataframe');if(!t.length)return;var table=t[0];var ths=table.tHead?table.tHead.rows[0].cells:[];for(let i=0;i<ths.length;i++){{ths[i].addEventListener('click',function(){{var asc=this.getAttribute('data-asc')!=='true';sortTable(table,i,!asc);this.setAttribute('data-asc',asc?'true':'false');}});}}}})();
</script>
</body></html>"""
    os.makedirs(os.path.dirname(out_html_path), exist_ok=True)
    with open(out_html_path, "w", encoding="utf-8") as f:
        f.write(html)

def clean_outputs(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    for pattern in ("*.html","*.csv","*.xlsx"):
        for f in glob.glob(os.path.join(out_dir, pattern)):
            try: os.remove(f)
            except Exception: pass

def apply_config_defaults(cfg, args_namespace):
    # Map YAML keys -> argparse attributes
    mapping = {
        "min_price": "min_price",
        "max_price": "max_price",
        "min_bedrooms": "min_bedrooms",
        "timeout": "timeout",
        "retries": "retries",
        "cycles_per_source": "cycles_per_source",
        "sleep_between": "sleep_between",
        "sleep_cycles": "sleep_cycles",
        "cooldown_secs": "cooldown_secs",
        "per_source_limit": "per_source_limit",
        "limit": "limit",
        "keywords": "keywords",
        "sources": "sources",
        "rotate_priority": "rotate_priority",
        "localities": "localities",
        "localities_file": "localities_file",
        "out_prefix": "out_prefix",
        "out_dir": "out_dir",
        "site_url": "site_url",
    }
    for k_yaml, k_arg in mapping.items():
        if k_yaml in cfg and getattr(args_namespace, k_arg) in (None, [], "", 0):
            setattr(args_namespace, k_arg, cfg[k_yaml])
    return args_namespace

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yml", help="YAML de configuração")
    ap.add_argument("--min-price", type=int, default=None)
    ap.add_argument("--max-price", type=int, default=None)
    ap.add_argument("--min-bedrooms", type=int, default=None)
    ap.add_argument("--sources", nargs="+", default=None, help="Ordem = prioridade")
    ap.add_argument("--rotate-priority", action="store_true")
    ap.add_argument("--keywords", nargs="*", default=None)
    ap.add_argument("--localities", nargs="+", default=None)
    ap.add_argument("--localities-file", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--per-source-limit", default=None)
    ap.add_argument("--timeout", type=int, default=None)
    ap.add_argument("--retries", type=int, default=None)
    ap.add_argument("--cycles-per-source", type=int, default=None)
    ap.add_argument("--sleep-between", type=float, default=None)
    ap.add_argument("--sleep-cycles", type=float, default=None)
    ap.add_argument("--cooldown-secs", type=int, default=None)
    ap.add_argument("--out-prefix", default=None)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--site-url", default=None)
    args = ap.parse_args()

    # Load YAML config
    cfg_path = args.config if args.config else "config.yml"
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {}

    # Apply config defaults
    args = apply_config_defaults(cfg, args)

    # Normalize types
    sources = args.sources if args.sources else list(SCRAPERS.keys())
    localities = read_localities(args.localities or [], args.localities_file)
    keywords = args.keywords or ["pedra","granito","xisto"]
    limit_global = int(args.limit) if args.limit not in (None, "") else 60
    timeout = int(args.timeout) if args.timeout else 35
    retries = int(args.retries) if args.retries else 5
    cycles = int(args.cycles_per_source) if args.cycles_per_source else 5
    sleep_between = float(args.sleep_between) if args.sleep_between else 4.0
    sleep_cycles = float(args.sleep_cycles) if args.sleep_cycles else 12.0
    cooldown_secs = int(args.cooldown_secs) if args.cooldown_secs else 900
    per_source_limit = parse_per_source_limit_map(args.per_source_limit) if args.per_source_limit else {}
    out_dir = args.out_dir or "docs"
    out_prefix = args.out_prefix or "data"
    site_url = args.site_url or "https://bytestay.github.io/serradaestrela/"
    rotate_priority = bool(args.rotate_priority or cfg.get("rotate_priority", False))

    # Rotation
    if rotate_priority and sources:
        today = datetime.datetime.now().strftime("%Y%m%d")
        shift = int(today) % len(sources)
        sources = sources[shift:] + sources[:shift]

    # Quotas
    if per_source_limit:
        quota_for = {s: int(per_source_limit.get(s, 0)) for s in sources}
    else:
        q_each = math.ceil(limit_global / max(1, len(sources)))
        quota_for = {s: q_each for s in sources}

    per_cycle_quota = {s: max(1, math.ceil(q for s,q in quota_for.items() and 1))}
    # fix per-cycle quota calc
    per_cycle_quota = {s: max(1, math.ceil(quota_for[s] / max(1, cycles))) for s in sources}

    # Prepare outputs
    prev_csv = os.path.join(out_dir, f"{out_prefix}.csv")
    df_prev = pd.read_csv(prev_csv) if os.path.exists(prev_csv) else None
    # Clean outputs
    os.makedirs(out_dir, exist_ok=True)
    for pattern in ("*.html","*.csv","*.xlsx"):
        for file in glob.glob(os.path.join(out_dir, pattern)):
            try: os.remove(file)
            except: pass

    # Round-robin with cooldowns
    all_items = []
    per_source_collected = {s: 0 for s in sources}
    blocked_until = {s: 0 for s in sources}

    for cycle in range(1, cycles+1):
        for sname in sources:
            now = time.time()
            if blocked_until[sname] > now:
                continue
            remaining = quota_for[sname] - per_source_collected[sname]
            if remaining <= 0:
                continue
            ask = min(per_cycle_quota[sname], remaining)
            fn = SCRAPERS[sname]
            try:
                items = fn(
                    int(args.max_price), int(args.min_price), int(args.min_bedrooms),
                    localities, keywords, ask, timeout, retries
                )
                if len(items) == 0:
                    blocked_until[sname] = now + cooldown_secs
                else:
                    per_source_collected[sname] += len(items)
                    all_items.extend(items)
            except Exception as e:
                blocked_until[sname] = now + cooldown_secs
            time.sleep(sleep_between + random.uniform(0, 0.7))
        time.sleep(sleep_cycles + random.uniform(0, 1.0))

    # Dedup
    seen, out = set(), []
    for it in all_items:
        key = (it.url or "").strip()
        if not key or key in seen: continue
        seen.add(key); out.append(it)

    df = pd.DataFrame([asdict(x) for x in out], columns=["source","title","price_eur","url","location","typology","details","image_url"])

    # Changes vs previous + "new" detection
    ups = downs = news = 0
    if df_prev is not None and not df.empty and "url" in df_prev.columns:
        prev = df_prev[["url","price_eur"]].rename(columns={"price_eur":"prev_price"})
        df = df.merge(prev, on="url", how="left")
        def badge_and_counts(p, pp):
            nonlocal ups, downs
            try:
                if pd.isna(pp): return ""
                delta = float(p) - float(pp)
                if abs(delta) < 1e-6: return ""
                if delta > 0: ups += 1
                else: downs += 1
                arrow = "↑" if delta > 0 else "↓"
                cls = "up" if delta > 0 else "down"
                return f'<span class="badge {cls}">{arrow} {delta:+.0f}€</span>'
            except Exception:
                return ""
        df["price_change"] = df.apply(lambda r: badge_and_counts(r["price_eur"], r["prev_price"]), axis=1)
        prev_urls = set(df_prev["url"].astype(str).tolist())
        news = sum(1 for u in df["url"].astype(str).tolist() if u not in prev_urls)
    else:
        df["prev_price"] = None
        df["price_change"] = ""
        news = len(df)  # first run: treat as new

    # Clickable title + image tag
    if not df.empty:
        df["title"] = df.apply(lambda r: f'<a href="{r["url"]}" target="_blank" rel="noopener">{r["title"]}</a>' if r.get("url") else r["title"], axis=1)
        df["photo"] = df["image_url"].apply(lambda u: f'<img src="{u}" alt="foto">' if isinstance(u,str) and u.startswith("http") else "")

    # Sort & columns
    order = ["photo","source","title","price_eur","price_change","location","typology","details","url","prev_price"]
    df_sorted = df[ [c for c in order if c in df.columns] ].sort_values(["price_eur"], ascending=[True], na_position="last")

    total_found = len(df_sorted)

    # Save outputs
    csv_path = os.path.join(out_dir, f"{out_prefix}.csv")
    xlsx_path = os.path.join(out_dir, f"{out_prefix}.xlsx")
    df_sorted.to_csv(csv_path, index=False)
    try:
        df_sorted.to_excel(xlsx_path, index=False)
    except Exception as e:
        print(f"[WARN] XLSX not written: {e}", file=sys.stderr)

    # HTML page
    out_html = os.path.join(out_dir, "index.html")
    write_html(df_sorted, out_html)

    # Minimal email summary (text)
    email_txt = f"""Resumo diário — Serra da Estrela (bytestay/serradaestrela)

Imóveis encontrados: {total_found}
Subidas de preço: {ups}
Descidas de preço: {downs}
Novos imóveis: {news}

Lista completa (com imagens e detalhes):
{site_url}
"""
    with open(os.path.join(out_dir, "email_summary.txt"), "w", encoding="utf-8") as f:
        f.write(email_txt)

    print(f"OK — {total_found} listings. Ups:{ups} Downs:{downs} New:{news}")
if __name__ == "__main__":
    main()
