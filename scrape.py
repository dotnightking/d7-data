"""
Data feed scraper v2 - public gaming commission data
Fetches prize data (CSV) + game details (HTML) + winner data (CSV)
Zero hardcoding. All data from texaslottery.com.
"""
import csv
import json
import io
import re
import urllib.request
import urllib.error
import time
import os
from datetime import datetime, timezone

BASE = "https://www.texaslottery.com/export/sites/lottery/Games/Scratch_Offs/"
CSV_URL = BASE + "scratchoff.csv"
INDEX_URL = BASE + "index.html"
WINNER_URL = BASE + "retailerswhosoldtopprizes{}.csv"

UA = {"User-Agent": "Mozilla/5.0 (compatible; DataSync/2.0)"}

def fetch(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"  Retry {i+1}/{retries}: {e}")
            time.sleep(2 * (i + 1))
    return None

def parse_csv(text):
    """Parse scratchoff.csv for prize tier data per game"""
    games = {}
    lines = text.strip().replace("\r\n", "\n").split("\n")
    start = 0
    for i, line in enumerate(lines):
        if "Game Number" in line:
            start = i
            break
    reader = csv.reader(io.StringIO("\n".join(lines[start:])))
    header = None
    for row in reader:
        if not header:
            header = [h.strip() for h in row]
            continue
        if len(row) < 7:
            continue
        d = dict(zip(header, [c.strip().strip('"') for c in row]))
        gn_str = d.get("Game Number", "").strip()
        if not gn_str or not gn_str.isdigit():
            continue
        gn = int(gn_str)
        if gn not in games:
            nm = d.get("Game Name", "")
            pr_str = d.get("Ticket Price", "0")
            pr = int(re.sub(r'[^\d]', '', pr_str) or "0")
            close_date = d.get("Game Close Date", "").strip()
            cs = 1 if close_date else 0
            games[gn] = {"gn": gn, "nm": nm, "pr": pr, "cs": cs,
                         "close_date": close_date, "tot": 0, "odds": 0,
                         "pk": 0, "guar": 0, "pz": []}
        level = d.get("Prize Level", "").strip()
        total_in_level = d.get("Total Prizes in Level", "0").strip().replace(",", "")
        claimed = d.get("Prizes Claimed", "0").strip().replace(",", "")
        tp = int(re.sub(r'[^\d]', '', total_in_level) or "0")
        cl = int(re.sub(r'[^\d]', '', claimed) or "0")
        if level.upper() == "TOTAL":
            pass  # Skip total row
        else:
            pa = int(re.sub(r'[^\d]', '', level) or "0")
            if pa > 0 and tp > 0:
                games[gn]["pz"].append({"a": pa, "p": tp, "c": cl})
    for g in games.values():
        g["pz"].sort(key=lambda x: -x["a"])
    return games

def parse_detail_page(html):
    """Extract game metadata from a detail page"""
    info = {}
    m = re.search(r'Game\s*(?:No\.?|Number|#)\s*(\d{3,5})', html, re.IGNORECASE)
    if m:
        info["gn"] = int(m.group(1))
    m = re.search(r'(?:approximately\s+)?([\d,]+)\*?\s*tickets\s+in\s+', html, re.IGNORECASE)
    if m:
        info["tot"] = int(m.group(1).replace(",", ""))
    m = re.search(r'Pack\s*Size[:\s]+(\d+)', html, re.IGNORECASE)
    if m:
        info["pk"] = int(m.group(1))
    m = re.search(r'Guaranteed\s+(?:Total\s+)?Prize\s+Amount\s*[=:]\s*\$?([\d,]+)', html, re.IGNORECASE)
    if m:
        info["guar"] = int(m.group(1).replace(",", ""))
    m = re.search(r'(?:Overall\s+)?odds\s+.*?1\s+in\s+([\d.]+)', html, re.IGNORECASE)
    if m:
        info["odds"] = float(m.group(1))
    return info

def find_detail_urls(html):
    """Extract detail page URLs from index page"""
    urls = []
    patterns = [
        r'href=["\']([^"\']*details\.html_[^"\']+)["\']',
        r'href=["\']([^"\']*details[^"\']*\.html[^"\']*)["\']',
    ]
    seen = set()
    for pat in patterns:
        for m in re.finditer(pat, html, re.IGNORECASE):
            url = m.group(1)
            if url not in seen and "details" in url.lower():
                seen.add(url)
                if url.startswith("http"):
                    urls.append(url)
                elif url.startswith("/"):
                    urls.append("https://www.texaslottery.com" + url)
                else:
                    urls.append(BASE + url)
    return list(set(urls))

def fetch_detail_for_games(games):
    """Fetch detail pages for all games missing metadata.
    Strategy: 
    1. Try index page to find detail URLs
    2. For games still missing, try direct URL pattern
    """
    # Step 1: index page
    print("  Fetching game listing page...")
    index_html = fetch(INDEX_URL)
    detail_urls = []
    if index_html:
        detail_urls = find_detail_urls(index_html)
        print(f"  Found {len(detail_urls)} detail URLs from index")
    
    matched = 0
    matched_gns = set()
    
    # Fetch URLs from index page
    for i, url in enumerate(detail_urls):
        print(f"  Detail {i+1}/{len(detail_urls)}...")
        html = fetch(url)
        if not html:
            continue
        info = parse_detail_page(html)
        gn = info.get("gn")
        if gn and gn in games:
            g = games[gn]
            if info.get("tot"): g["tot"] = info["tot"]
            if info.get("pk"): g["pk"] = info["pk"]
            if info.get("guar"): g["guar"] = info["guar"]
            if info.get("odds"): g["odds"] = info["odds"]
            matched += 1
            matched_gns.add(gn)
        time.sleep(0.5)
    
    # Step 2: direct URL pattern for games still missing data
    missing = [gn for gn, g in games.items() 
               if gn not in matched_gns and (g["tot"] == 0 or g["pk"] == 0)]
    if missing:
        print(f"  Trying direct URLs for {len(missing)} games still missing data...")
        for gn in missing:
            # Try common URL patterns
            patterns = [
                f"{BASE}details.html_{gn}.html",
                f"{BASE}details_{gn}.html",
            ]
            for url in patterns:
                html = fetch(url)
                if not html or len(html) < 200:
                    continue
                info = parse_detail_page(html)
                if info.get("gn") == gn or info.get("tot"):
                    g = games[gn]
                    if info.get("tot"): g["tot"] = info["tot"]
                    if info.get("pk"): g["pk"] = info["pk"]
                    if info.get("guar"): g["guar"] = info["guar"]
                    if info.get("odds"): g["odds"] = info["odds"]
                    matched += 1
                    print(f"    #{gn} matched via direct URL")
                    break
                time.sleep(0.3)
            time.sleep(0.3)
    
    return matched

def fetch_winners(game_numbers):
    """Fetch winner/retailer CSVs"""
    all_w = {}
    for gn in game_numbers:
        url = WINNER_URL.format(gn)
        print(f"  Winners #{gn}...")
        text = fetch(url)
        if not text or "404" in text[:100].lower() or "not found" in text[:200].lower():
            continue
        entries = []
        try:
            reader = csv.reader(io.StringIO(text))
            header = None
            for row in reader:
                if not header:
                    header = [h.strip() for h in row]
                    continue
                if len(row) < 5:
                    continue
                d = dict(zip(header, [c.strip() for c in row]))
                w = {
                    "date": d.get("Date Claimed", ""),
                    "store": d.get("Selling Retailer", ""),
                    "addr": d.get("Selling Retailer Address", ""),
                    "city": d.get("Selling Retailer City", ""),
                    "zip": d.get("Selling Retailer Zip Code", ""),
                    "pn": int(re.sub(r'[^\d]', '', d.get("Pack Number", "0")) or "0"),
                    "tk": int(re.sub(r'[^\d]', '', d.get("Ticket Number", "0")) or "0"),
                }
                if w["date"] and w["store"]:
                    entries.append(w)
        except Exception as e:
            print(f"    Parse error: {e}")
        if entries:
            all_w[str(gn)] = entries
        time.sleep(0.5)
    return all_w

def main():
    os.makedirs("data", exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== Feed sync v2 — {now} ===")

    # Step 1: CSV
    print("Step 1: Fetching prize data CSV...")
    csv_text = fetch(CSV_URL)
    games = {}
    if csv_text:
        games = parse_csv(csv_text)
        print(f"  Parsed {len(games)} games")
        with open("data/raw.csv", "w") as f:
            f.write(csv_text)
    if not games:
        print("ERROR: No CSV data")
        return

    # Step 2+3: Detail pages
    print("Step 2: Fetching detail pages...")
    matched = fetch_detail_for_games(games)
    print(f"  Matched metadata for {matched} games")

    missing = [g for g in games.values() if g["tot"] == 0 or g["pk"] == 0]
    if missing:
        print(f"  WARNING: {len(missing)} games still missing detail data")
        for g in missing[:10]:
            print(f"    #{g['gn']} {g['nm']}")

    # Step 3: Winners
    with_claims = [gn for gn, g in games.items() if g["pz"] and g["pz"][0]["c"] > 0]
    print(f"Step 3: Fetching winner data for {len(with_claims)} games...")
    winners = fetch_winners(with_claims)
    print(f"  Got winners for {len(winners)} games")

    # Output
    output = {
        "updated": now,
        "game_count": len(games),
        "games": list(games.values()),
        "winners": winners,
        "winner_count": sum(len(v) for v in winners.values()),
    }

    with open("data/feed.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"Saved data/feed.json ({os.path.getsize('data/feed.json')} bytes)")

    with open("data/wdata.json", "w") as f:
        json.dump({"updated": now, "winners": winners}, f, separators=(",", ":"))

    has_tot = sum(1 for g in games.values() if g.get("tot", 0) > 0)
    has_pk = sum(1 for g in games.values() if g.get("pk", 0) > 0)
    print(f"\n=== Done ===")
    print(f"  Games: {len(games)}")
    print(f"  With total tickets: {has_tot}")
    print(f"  With pack size: {has_pk}")
    print(f"  Winner records: {output['winner_count']}")

if __name__ == "__main__":
    main()
