#!/usr/bin/env python3
"""Write a ClickBench-shaped `hits.csv` for `perf/clickbench/create.sql`.

The real ClickBench dataset lives on datasets.clickhouse.com. Use this
generator only when that host cannot be reached. The row counts, the
distinct-value counts and the share of empty strings copy the published
profile of the first 1,000,000 rows of `hits_compatible/hits.csv`, so the
queries keep the same selectivities. The text itself is not the real text.
"""

import argparse
import csv
import os

import numpy as np


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=20260917)
    parser.add_argument("--create-sql", default="perf/clickbench/create.sql")
    parser.add_argument("--out", default="perf/clickbench/hits.csv")
    args = parser.parse_args()

    columns = read_columns(args.create_sql)
    rows = args.rows
    rng = np.random.default_rng(args.seed)

    # CounterID 62 owns about 9 percent of the real head of the dataset, and a
    # long zipf tail owns the rest. Queries 37 to 43 all filter on CounterID 62.
    counter_tail = 1 + zipf_pool(rng, 3000, rows)
    is_hot_counter = rng.random(rows) < 0.09
    counter_id = np.where(is_hot_counter, 62, counter_tail)

    day_of_month = rng.integers(1, 32, size=rows)
    event_date = np.array([f"2013-07-{d:02d}" for d in day_of_month])
    seconds = rng.integers(0, 86400, size=rows)
    event_time = np.array(
        [
            f"2013-07-{d:02d} {s // 3600:02d}:{(s // 60) % 60:02d}:{s % 60:02d}"
            for d, s in zip(day_of_month, seconds)
        ]
    )

    user_pool = rng.integers(10**17, 9 * 10**17, size=200_000, dtype=np.int64)
    user_pool[0] = 435090932899640449  # query 20 looks this one up
    user_id = user_pool[zipf_pool(rng, len(user_pool), rows)]
    watch_id = rng.integers(10**17, 9 * 10**17, size=rows, dtype=np.int64)

    client_ip_pool = rng.integers(0, 2**31 - 1, size=120_000, dtype=np.int64)
    client_ip = client_ip_pool[zipf_pool(rng, len(client_ip_pool), rows)]

    region_id = zipf_pool(rng, 600, rows)
    search_engine_id = np.where(rng.random(rows) < 0.11, rng.integers(1, 40, size=rows), 0)
    adv_engine_id = np.where(rng.random(rows) < 0.04, rng.integers(1, 40, size=rows), 0)
    trafic_source_id = rng.choice([-1, 0, 1, 2, 3, 6, 8], size=rows, p=[0.09, 0.5, 0.11, 0.1, 0.09, 0.06, 0.05])

    url_pool = text_pool("http://example.", 40_000, 44, rng)
    for i in range(0, len(url_pool), 50):  # about 2 percent of URLs name google
        url_pool[i] = "http://www.google.com/search?q=" + url_pool[i][-24:]
    referer_pool = text_pool("http://ref.", 20_000, 40, rng)
    referer_pool[0] = ""
    title_pool = text_pool("Page ", 30_000, 42, rng)
    for i in range(0, len(title_pool), 100):
        title_pool[i] = "Google " + title_pool[i][-34:]
    phrase_pool = text_pool("q", 20_000, 18, rng)
    model_pool = text_pool("model-", 400, 8, rng)

    url_idx = zipf_pool(rng, len(url_pool), rows)
    referer_idx = zipf_pool(rng, len(referer_pool), rows)
    title_idx = zipf_pool(rng, len(title_pool), rows)
    phrase_idx = zipf_pool(rng, len(phrase_pool), rows)
    model_idx = zipf_pool(rng, len(model_pool), rows)

    has_url = rng.random(rows) < 0.93
    has_referer = rng.random(rows) < 0.62
    has_phrase = rng.random(rows) < 0.13
    has_model = rng.random(rows) < 0.04

    url_hash_pool = rng.integers(10**17, 9 * 10**18, size=50_000, dtype=np.int64)
    url_hash_pool[0] = 2868770270353813622  # query 42 looks this one up
    referer_hash_pool = rng.integers(10**17, 9 * 10**18, size=40_000, dtype=np.int64)
    referer_hash_pool[0] = 3594120000172545465  # query 41 looks this one up
    url_hash = url_hash_pool[zipf_pool(rng, len(url_hash_pool), rows)]
    referer_hash = referer_hash_pool[zipf_pool(rng, len(referer_hash_pool), rows)]

    def small(high):
        return rng.integers(0, high, size=rows)

    def flag(share):
        return (rng.random(rows) < share).astype(np.int64)

    values = {
        "WatchID": watch_id,
        "JavaEnable": flag(0.86),
        "Title": np.where(has_url, np.array(title_pool)[title_idx], ""),
        "GoodEvent": np.ones(rows, dtype=np.int64),
        "EventTime": event_time,
        "EventDate": event_date,
        "CounterID": counter_id,
        "ClientIP": client_ip,
        "RegionID": region_id,
        "UserID": user_id,
        "CounterClass": small(3),
        "OS": small(60),
        "UserAgent": small(10),
        "URL": np.where(has_url, np.array(url_pool)[url_idx], ""),
        "Referer": np.where(has_referer, np.array(referer_pool)[referer_idx], ""),
        "IsRefresh": flag(0.08),
        "RefererCategoryID": small(2000),
        "RefererRegionID": small(600),
        "URLCategoryID": small(2000),
        "URLRegionID": small(600),
        "ResolutionWidth": rng.integers(0, 2000, size=rows),
        "ResolutionHeight": rng.integers(0, 1200, size=rows),
        "ResolutionDepth": small(40),
        "FlashMajor": small(20),
        "FlashMinor": small(12),
        "FlashMinor2": np.array(["800", "700", "0", "202"])[small(4)],
        "NetMajor": small(5),
        "NetMinor": small(5),
        "UserAgentMajor": small(30),
        "UserAgentMinor": np.array(["ii", "n2", "2b", ""])[small(4)],
        "CookieEnable": flag(0.98),
        "JavascriptEnable": flag(0.96),
        "IsMobile": flag(0.05),
        "MobilePhone": np.where(has_model, small(80), 0),
        "MobilePhoneModel": np.where(has_model, np.array(model_pool)[model_idx], ""),
        "Params": np.where(rng.random(rows) < 0.05, "utm=1", ""),
        "IPNetworkID": small(2**20),
        "TraficSourceID": trafic_source_id,
        "SearchEngineID": search_engine_id,
        "SearchPhrase": np.where(has_phrase, np.array(phrase_pool)[phrase_idx], ""),
        "AdvEngineID": adv_engine_id,
        "IsArtifical": flag(0.02),
        "WindowClientWidth": rng.integers(0, 1900, size=rows),
        "WindowClientHeight": rng.integers(0, 1000, size=rows),
        "ClientTimeZone": rng.integers(-720, 780, size=rows),
        "ClientEventTime": event_time,
        "SilverlightVersion1": small(6),
        "SilverlightVersion2": small(6),
        "SilverlightVersion3": small(30000),
        "SilverlightVersion4": small(400),
        "PageCharset": np.array(["utf-8", "windows-1251", ""])[small(3)],
        "CodeVersion": small(20000),
        "IsLink": flag(0.1),
        "IsDownload": flag(0.01),
        "IsNotBounce": flag(0.3),
        "FUniqID": rng.integers(0, 9 * 10**18, size=rows, dtype=np.int64),
        "OriginalURL": np.where(rng.random(rows) < 0.03, np.array(url_pool)[url_idx], ""),
        "HID": small(2**24),
        "IsOldCounter": flag(0.01),
        "IsEvent": flag(0.01),
        "IsParameter": flag(0.01),
        "DontCountHits": flag(0.03),
        "WithHash": flag(0.02),
        "HitColor": np.array(["5", "b", "a"])[small(3)],
        "LocalEventTime": event_time,
        "Age": small(56),
        "Sex": small(2),
        "Income": small(6),
        "Interests": small(2**14),
        "Robotness": small(3),
        "RemoteIP": client_ip,
        "WindowName": rng.integers(-32000, 32000, size=rows),
        "OpenerName": rng.integers(-32000, 32000, size=rows),
        "HistoryLength": rng.integers(-1, 20, size=rows),
        "BrowserLanguage": np.array(["ru", "en", "uk", ""])[small(4)],
        "BrowserCountry": np.array(["RU", "UA", "US", ""])[small(4)],
        "SocialNetwork": np.where(rng.random(rows) < 0.02, "vkontakte", ""),
        "SocialAction": np.where(rng.random(rows) < 0.02, "share", ""),
        "HTTPError": np.where(rng.random(rows) < 0.01, 404, 0),
        "SendTiming": small(3000),
        "DNSTiming": small(600),
        "ConnectTiming": small(600),
        "ResponseStartTiming": small(3000),
        "ResponseEndTiming": small(3000),
        "FetchTiming": small(3000),
        "SocialSourceNetworkID": small(8),
        "SocialSourcePage": np.where(rng.random(rows) < 0.02, "wall", ""),
        "ParamPrice": np.zeros(rows, dtype=np.int64),
        "ParamOrderID": np.full(rows, ""),
        "ParamCurrency": np.where(rng.random(rows) < 0.01, "RUR", ""),
        "ParamCurrencyID": small(3),
        "OpenstatServiceName": np.full(rows, ""),
        "OpenstatCampaignID": np.full(rows, ""),
        "OpenstatAdID": np.full(rows, ""),
        "OpenstatSourceID": np.full(rows, ""),
        "UTMSource": np.where(rng.random(rows) < 0.03, "yandex", ""),
        "UTMMedium": np.where(rng.random(rows) < 0.03, "cpc", ""),
        "UTMCampaign": np.full(rows, ""),
        "UTMContent": np.full(rows, ""),
        "UTMTerm": np.full(rows, ""),
        "FromTag": np.full(rows, ""),
        "HasGCLID": flag(0.01),
        "RefererHash": referer_hash,
        "URLHash": url_hash,
        "CLID": small(2**20),
    }

    missing = [c for c in columns if c not in values]
    if missing:
        raise SystemExit(f"generator has no values for: {missing}")

    # The real dataset is stored in primary-key order, which decides how
    # sequential the table reads after an index seek are. Copy that order.
    order = np.lexsort((watch_id, event_time, user_id, event_date, counter_id))

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="") as handle:
        writer = csv.writer(handle)
        block = 50_000
        table = [values[c] for c in columns]
        for start in range(0, rows, block):
            idx = order[start : start + block]
            writer.writerows(zip(*[col[idx] for col in table]))
    print(f"wrote {rows} rows to {args.out}")


def read_columns(create_sql_path):
    columns = []
    inside = False
    for line in open(create_sql_path):
        stripped = line.strip()
        if stripped.upper().startswith("CREATE TABLE"):
            inside = True
            continue
        if not inside:
            continue
        if stripped.upper().startswith("PRIMARY KEY"):
            break
        parts = stripped.rstrip(",").split()
        if len(parts) >= 2:
            columns.append(parts[0])
    return columns


def zipf_pool(rng, pool_size, rows, exponent=1.15):
    ranks = rng.zipf(exponent, size=rows)
    return np.minimum(ranks, pool_size) - 1


def text_pool(prefix, count, width, rng):
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_/"
    letters = np.array(list(alphabet))
    body = rng.choice(letters, size=(count, width))
    return [prefix + "".join(row) for row in body]


if __name__ == "__main__":
    main()
