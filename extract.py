import os
import re
import json
import pandas as pd
from bs4 import BeautifulSoup
import nltk
from textblob import TextBlob

# ============================================================
# ★ 目录设置（按你的要求）
# ============================================================
SEO_DIR = "outputs_etsy_final"
GEO_DIR = "outputs_GEO_final"

MASTER_INDEX = "master_index.csv"
JSON_DATA_FILE = "json_data.csv"
MD_DATA_FILE = "md_data.csv"
HTML_DATA_FILE = "html_data.csv"
MASTER_MERGED = "master_merged.csv"

STYLE_KEYWORDS = ["y2k", "retro", "coquette", "minimalist", "modern",
                  "cottagecore", "boho", "preppy", "aesthetic"]

PERSUASION_KEYWORDS = ["Instant download", "Sale", "Limited",
                       "Fast", "High-resolution", "Perfect for"]


# ============================================================
# STEP 1 — 构建 master index
# ============================================================
def build_master_index():
    rows = []

    for channel_name, base_dir in [("SEO", SEO_DIR), ("GEO", GEO_DIR)]:
        if not os.path.exists(base_dir):
            print(f"⚠️ 找不到目录：{base_dir}")
            continue

        for query_id in os.listdir(base_dir):
            q_dir = os.path.join(base_dir, query_id)
            if not os.path.isdir(q_dir):
                continue

            files = os.listdir(q_dir)
            slugs = set()

            for f in files:
                match = re.match(r"(.+?)(_full)?\.(md|html|json)$", f)
                if match:
                    slugs.add(match.group(1))

            for rank, slug in enumerate(sorted(slugs), 1):
                rows.append({
                    "query_id": query_id,
                    "channel": channel_name,
                    "rank": rank,
                    "url_slug": slug,
                    "json_path": os.path.join(q_dir, f"{slug}_full.json"),
                    "md_path": os.path.join(q_dir, f"{slug}.md"),
                    "html_path": os.path.join(q_dir, f"{slug}.html"),
                })

    df = pd.DataFrame(rows)
    df.to_csv(MASTER_INDEX, index=False)
    print(f"✅ STEP 1 完成: {MASTER_INDEX} ({len(df)} rows)")
    return df


# ============================================================
# STEP 2 — JSON 数据提取（已修复全部 NoneType 问题）
# ============================================================
def extract_json(row):

    out = {
        "query_id": row["query_id"],
        "channel": row["channel"],
        "rank": row["rank"],
    }

    try:
        with open(row["json_path"], encoding="utf-8") as f:
            j = json.load(f)
    except:
        return out  # skip

    # 安全 get
    def g(k, default=None):
        v = j.get(k, default)
        return v

    # 安全解析价格
    def parse_price(v):
        if not isinstance(v, str):
            return None
        m = re.search(r"[\d.]+", v)
        return float(m.group()) if m else None

    price = parse_price(g("price"))
    original = parse_price(g("originalPrice"))
    discount = round((original - price) / original, 4) if original and price and price < original else 0

    # images
    imgs = g("images") or []
    if not isinstance(imgs, list):
        imgs = []

    # payment methods
    payment_methods = g("paymentMethods") or []
    if not isinstance(payment_methods, list):
        payment_methods = []

    # faq
    faq_items = g("faq_items") or []
    if not isinstance(faq_items, list):
        faq_items = []

    # description
    description_text = g("description_text") or ""
    if not isinstance(description_text, str):
        description_text = ""

    out.update({
        "title": g("title") or None,
        "price": price,
        "originalPrice": original,
        "discount": discount,
        "rating": g("rating"),
        "reviewsCount": g("reviewsCount"),
        "bestseller": bool(g("bestseller")),
        "starSeller": bool(g("starSeller")),
        "shop": g("shop"),
        "delivery": g("delivery"),
        "shopPolicies": "yes" if g("shopPolicies") else "no",
        "purchaseProtection": "yes" if g("purchaseProtection") else "no",
        "return_policy_text": g("return_policy_text", "") or "",
        "paymentMethods": json.dumps(payment_methods, ensure_ascii=False),
        "paymentMethods_count": len(payment_methods),

        "number_of_images": len(imgs),
        "first_image_url": imgs[0].get("url") if imgs and isinstance(imgs[0], dict) else None,
        "alt_text_available": any(isinstance(img, dict) and img.get("alt_text") for img in imgs),

        "description_text": description_text,
        "description_word_count": len(description_text.split()),
        "description_char_count": len(description_text),

        "number_of_faq_items": len(faq_items),
    })

    return out


# ============================================================
# STEP 3 — Markdown 数据提取
# ============================================================
def extract_md(row):

    out = {
        "query_id": row["query_id"],
        "channel": row["channel"],
        "rank": row["rank"],
    }

    try:
        with open(row["md_path"], encoding="utf-8") as f:
            text = f.read()
    except:
        return out

    clean = re.sub(r"[#*\[\]\(\)]", " ", text)
    words = clean.split()
    sentences = nltk.sent_tokenize(clean) if clean.strip() else []

    lower_text = text.lower()
    style_hits = [k for k in STYLE_KEYWORDS if k in lower_text]
    persuasion_count = sum(lower_text.count(k.lower()) for k in PERSUASION_KEYWORDS)

    cat_match = re.search(r"Category:\s*(.+)", text)
    cat_path = cat_match.group(1).strip() if cat_match else None
    top_cat = cat_path.split(">")[0].strip() if cat_path else None

    out.update({
        "md_word_count": len(words),
        "md_sentence_count": len(sentences),
        "md_avg_sentence_length": len(words) / len(sentences) if sentences else 0,
        "md_num_bullets": len(re.findall(r"^\s*[-*]\s", text, re.MULTILINE)),
        "md_num_sections": len(re.findall(r"^#+\s", text, re.MULTILINE)),
        "style_descriptor_count": len(style_hits),
        "style_descriptors_unique": len(set(style_hits)),
        "persuasion_word_count": persuasion_count,
        "md_sentiment_polarity": TextBlob(clean).sentiment.polarity,
        "md_category_path": cat_path,
        "md_top_category": top_cat,
    })

    return out


# ============================================================
# STEP 4 — HTML 数据提取
# ============================================================
def extract_html(row):
    out = {
        "query_id": row["query_id"],
        "channel": row["channel"],
        "rank": row["rank"],
    }

    try:
        with open(row["html_path"], encoding="utf-8") as f:
            html = f.read()
    except:
        return out

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True).lower()

    bc = []
    nav = soup.find("nav", {"aria-label": re.compile("breadcrumb", re.I)})
    if nav:
        bc = [a.get_text(strip=True) for a in nav.find_all("a")]

    price_elem = soup.find("span", class_=re.compile("price|currency", re.I))
    price_val = None
    if price_elem:
        m = re.search(r"[\d.]+", price_elem.get_text())
        if m:
            price_val = float(m.group())

    carts_match = re.search(r"(\d+)\s+people\s+have\s+this\s+in\s+carts", text)
    carts = int(carts_match.group(1)) if carts_match else 0

    alts = [img.get("alt") for img in soup.find_all("img") if img.get("alt")]

    desc_div = soup.find(id=re.compile("description|details", re.I))
    desc = desc_div.get_text(strip=True) if desc_div else ""

    out.update({
        "html_category_path": " > ".join(bc),
        "html_top_category": bc[0] if bc else None,
        "html_category_depth": len(bc),
        "html_price": price_val,
        "html_star_seller_flag": "star seller" in text,
        "html_low_stock_flag": "only" in text and "left in stock" in text,
        "html_in_carts_count": carts,
        "image_alt_text": " ".join(alts),
        "image_alt_keyword_count": len(" ".join(alts).split()),
        "html_description_text": desc,
        "html_description_word_count": len(desc.split()),
    })

    return out


# ============================================================
# STEP 5 — 合并数据
# ============================================================
def run_phase1():
    print("========== PHASE 1 START ==========")

    df_index = build_master_index()
    if df_index.empty:
        print("❌ No files found.")
        return

    print("👉 Running JSON extraction...")
    df_json = pd.DataFrame([extract_json(r) for _, r in df_index.iterrows()])
    df_json.to_csv(JSON_DATA_FILE, index=False)
    print(f"✔ Saved {JSON_DATA_FILE}")

    print("👉 Running MD extraction...")
    df_md = pd.DataFrame([extract_md(r) for _, r in df_index.iterrows()])
    df_md.to_csv(MD_DATA_FILE, index=False)
    print(f"✔ Saved {MD_DATA_FILE}")

    print("👉 Running HTML extraction...")
    df_html = pd.DataFrame([extract_html(r) for _, r in df_index.iterrows()])
    df_html.to_csv(HTML_DATA_FILE, index=False)
    print(f"✔ Saved {HTML_DATA_FILE}")

    print("👉 Merging all data...")
    keys = ["query_id", "channel", "rank"]
    df_master = df_json.merge(df_md, on=keys, how="left").merge(df_html, on=keys, how="left")
    df_master.to_csv(MASTER_MERGED, index=False)

    print(f"🎉 PHASE 1 完成: {MASTER_MERGED}")
    return df_master


if __name__ == "__main__":
    run_phase1()
