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
from datetime import datetime, timezone

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
    """Parse scratchoff.csv — real format:
    Row 1: title like 'Scratch-Off Prizes as of 02/09/2026'
    Row 2: headers: Game Number, Game Name, Game Close Date, Ticket Price, Prize Level, Total Prizes in Level, Prizes Claimed
    Prize Level = dollar amount (1, 2, 5, 500, etc.) or 'TOTAL'
    TOTAL row has total tickets printed / claimed for the game
    """
    games = {}
    lines = text.strip().replace("\r\n", "\n").split("\n")

    # Skip title row (first row is "Scratch-Off Prizes as of...")
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
            games[gn] = {"gn": gn, "nm": nm, "pr": pr, "cs": cs, "close_date": close_date, "tot": 0, "odds": 0, "pz": []}

        level = d.get("Prize Level", "").strip()
        total_in_level = d.get("Total Prizes in Level", "0").strip().replace(",", "")
        claimed = d.get("Prizes Claimed", "0").strip().replace(",", "")

        tp = int(re.sub(r'[^\d]', '', total_in_level) or "0")
        cl = int(re.sub(r'[^\d]', '', claimed) or "0")

        if level.upper() == "TOTAL":
            # TOTAL row: total tickets printed and claimed
            games[gn]["tot"] = tp
            # Calculate odds from total tickets and total prize-winning tickets
            prize_tickets = sum(p["p"] for p in games[gn]["pz"])
            if prize_tickets > 0:
                games[gn]["odds"] = round(tp / prize_tickets, 2)
        else:
            # Prize tier row
            pa = int(re.sub(r'[^\d]', '', level) or "0")
            if pa > 0 and tp > 0:
                games[gn]["pz"].append({"a": pa, "p": tp, "c": cl})

    # Sort prize tiers descending by amount
    for g in games.values():
        g["pz"].sort(key=lambda x: -x["a"])

    return games

def fetch_detail(game_numbers):
    """Fetch winner/retailer CSVs for all games"""
    all_w = {}
    for gn in game_numbers:
        url = F2.format(gn)
        print(f"  Detail #{gn}...")
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
        print("ERROR: No data retrieved")
        return

    # Find games with claimed top prizes (first prize tier has claims)
    with_claims = []
    for gn, g in games.items():
        if g["pz"] and g["pz"][0]["c"] > 0:
            with_claims.append(gn)
    print(f"Entries with detail data: {len(with_claims)}")

    print("Fetching detail data...")
    detail = fetch_detail(with_claims)
    print(f"  Got detail for {len(detail)} entries")

    # Build output
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
