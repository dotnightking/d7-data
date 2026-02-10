"""
Data feed scraper - public gaming commission data
"""
import csv
import json
import io
import re
import urllib.request
import urllib.error
import time
import os
from datetime import datetime

SRC = "https://www.texaslottery.com/export/sites/lottery/Games/Scratch_Offs/"
F1 = SRC + "scratchoff.csv"
F2 = SRC + "retailerswhosoldtopprizes{}.csv"

def fetch(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; DataSync/1.0)"
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            print(f"  Retry {i+1}/{retries}: {e}")
            time.sleep(2)
    return None

def parse_csv(text):
    games = {}
    reader = csv.reader(io.StringIO(text))
    header = None
    for row in reader:
        if not header:
            header = [h.strip() for h in row]
            continue
        if len(row) < len(header):
            continue
        d = dict(zip(header, [c.strip() for c in row]))
        gn_str = d.get("Game Number", d.get("Game #", "")).strip()
        if not gn_str or not gn_str.isdigit():
            continue
        gn = int(gn_str)
        if gn not in games:
            pr_str = d.get("Ticket Price", d.get("Price", "$0"))
            pr = int(re.sub(r'[^\d]', '', pr_str) or "0")
            sd = d.get("Start Date", d.get("Start", ""))
            nm = d.get("Game Name", d.get("Name", ""))
            tot_str = d.get("Total Tickets", d.get("Approximate Tickets", "0"))
            tot = int(re.sub(r'[^\d]', '', tot_str) or "0")
            odds_str = d.get("Overall Odds", d.get("Odds", "0"))
            odds_match = re.search(r'[\d.]+', odds_str.replace(",", ""))
            odds = float(odds_match.group()) if odds_match else 0
            cs_str = d.get("Closing", d.get("Status", ""))
            cs = 1 if "closing" in cs_str.lower() or "*" in cs_str else 0
            games[gn] = {"gn": gn, "nm": nm, "pr": pr, "tot": tot, "odds": odds, "sd": sd, "cs": cs, "pz": []}
        pa_str = d.get("Prize Amount", d.get("Prize", "0"))
        pa = int(re.sub(r'[^\d]', '', pa_str) or "0")
        pp_str = d.get("Prizes Printed", d.get("Total Prizes", "0"))
        pp = int(re.sub(r'[^\d]', '', pp_str) or "0")
        pc_str = d.get("Prizes Claimed", d.get("Claimed", "0"))
        pc = int(re.sub(r'[^\d]', '', pc_str) or "0")
        if pa > 0 and pp > 0:
            games[gn]["pz"].append({"a": pa, "p": pp, "c": pc})
    for g in games.values():
        g["pz"].sort(key=lambda x: -x["a"])
    return games

def parse_html(text):
    games = {}
    current_gn = None
    for line in text.split("\n"):
        gn_match = re.search(r'\[(\d{4})\]', line)
        if gn_match:
            current_gn = int(gn_match.group(1))
            date_match = re.search(r'\|\s*(\d{2}/\d{2}/\d{2})\s*\|', line)
            sd = date_match.group(1) if date_match else ""
            pr_match = re.search(r'\$(\d+)\s*\|', line)
            pr = int(pr_match.group(1)) if pr_match else 0
            cs = 1 if "\\*" in line or "* |" in line else 0
            nm_match = re.search(r'\|\s*(?:\\\*)?\s*\|\s*(.+?)\s*\|\s*\$', line)
            nm = nm_match.group(1).strip() if nm_match else ""
            if current_gn not in games:
                games[current_gn] = {"gn": current_gn, "nm": nm, "pr": pr, "sd": sd, "cs": cs, "tot": 0, "odds": 0, "pz": []}
        if current_gn and "|" in line:
            pz_match = re.findall(r'\$[\d,]+\s*\|\s*[\d,]+\s*\|\s*[\d,\-]+', line)
            for pm in pz_match:
                nums = re.findall(r'[\d,]+', pm)
                if len(nums) >= 3:
                    pa = int(nums[0].replace(",", ""))
                    pp = int(nums[1].replace(",", ""))
                    pc_str = nums[2].replace(",", "")
                    pc = int(pc_str) if pc_str.isdigit() else 0
                    if current_gn in games:
                        games[current_gn]["pz"].append({"a": pa, "p": pp, "c": pc})
    for g in games.values():
        g["pz"].sort(key=lambda x: -x["a"])
    return games

def fetch_detail(game_numbers):
    all_w = {}
    for gn in game_numbers:
        url = F2.format(gn)
        print(f"  Detail #{gn}...")
        text = fetch(url)
        if not text:
            continue
        entries = []
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
        if entries:
            all_w[str(gn)] = entries
        time.sleep(0.5)
    return all_w

def main():
    os.makedirs("data", exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== Feed sync — {now} ===")

    print("Fetching primary source...")
    csv_text = fetch(F1)
    games = {}
    if csv_text:
        games = parse_csv(csv_text)
        print(f"  Parsed {len(games)} entries from CSV")
        with open("data/raw.csv", "w") as f:
            f.write(csv_text)

    if not games:
        print("CSV failed, trying HTML...")
        html_text = fetch(SRC + "all.html")
        if html_text:
            games = parse_html(html_text)
            print(f"  Parsed {len(games)} entries from HTML")

    if not games:
        print("ERROR: No data retrieved")
        return

    with_claims = [gn for gn, g in games.items() if g["pz"] and g["pz"][0]["c"] > 0]
    print(f"Entries with detail data: {len(with_claims)}")

    print("Fetching detail data...")
    detail = fetch_detail(with_claims)
    print(f"  Got detail for {len(detail)} entries")

    output = {
        "updated": now,
        "game_count": len(games),
        "games": list(games.values()),
        "winners": detail,
        "winner_count": sum(len(v) for v in detail.values()),
    }

    with open("data/feed.json", "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"Saved data/feed.json ({os.path.getsize('data/feed.json')} bytes)")

    with open("data/wdata.json", "w") as f:
        json.dump({"updated": now, "winners": detail}, f, separators=(",", ":"))

    print(f"=== Done: {len(games)} entries, {output['winner_count']} detail records ===")

if __name__ == "__main__":
    main()
