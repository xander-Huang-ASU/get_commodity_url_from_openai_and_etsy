import os
import re
import time
import json
import csv
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from firecrawl import Firecrawl

# ========== ⚙️ 环境配置 ==========
load_dotenv()

FIRECRAWL_API_KEY = ""
# if not FIRECRAWL_API_KEY:
#     raise ValueError("❌ 请先在 .env 文件中设置 FIRECRAWL_API_KEY")

# ========== ⚙️ 常量配置 ==========
EXCEL_FILE = "prompts.xlsx"
OUTPUT_FILE = "outputs_etsy/etsy_firecrawl_summary.csv"
BASE_DIR = "outputs_etsy"
MAX_URLS = 3
MAX_RETRIES = 3
RETRY_DELAY = 3  # 秒
TIMEOUT = 300000    # Firecrawl超时（毫秒）

os.makedirs(BASE_DIR, exist_ok=True)
firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)


# ========== Firecrawl 抓取函数 ==========
def fetch_with_firecrawl(url, prompt_dir):
    import random
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", url[:80])
    md_path = os.path.join(prompt_dir, f"{safe_name}.md")
    html_path = os.path.join(prompt_dir, f"{safe_name}.html")
    json_path = os.path.join(prompt_dir, f"{safe_name}_full.json")

    base_timeout = TIMEOUT
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🕸️ Firecrawl 抓取第 {attempt}/{MAX_RETRIES} 次: {url}")
        try:
            start = time.time()

            result = firecrawl.scrape(
                url,
                formats=[
                    "markdown",
                    "html",
                    {
                        "type": "json",
                        "schema": {"type": "object", "properties": {}}
                    }
                ],
                only_main_content=False,
                timeout=base_timeout
            )
            elapsed = round(time.time() - start, 2)

            if result.html and "Please enable JS" in result.html:
                print("⚠️ 检测到反爬页面（需要启用 JS），准备延迟后重试...")
                delay = random.uniform(10, 20)
                print(f"⏳ 等待 {round(delay, 1)} 秒再试...")
                time.sleep(delay)
                continue

            with open(md_path, "w", encoding="utf-8") as f:
                f.write(result.markdown or "")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(result.html or "")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(result.json or {}, f, ensure_ascii=False, indent=2)

            print(f"✅ 抓取成功 [{elapsed}s] 已保存 Markdown / HTML / JSON")
            return {
                "url": url,
                "status": "success",
                "markdown_file": md_path,
                "html_file": html_path,
                "json_file": json_path,
                "attempt": attempt,
                "elapsed_s": elapsed,
                "time_s": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

        except Exception as e:
            print(f"❌ Firecrawl 抓取失败: {e}")
            delay = RETRY_DELAY * attempt + random.uniform(2, 5)
            base_timeout = int(base_timeout * 1.5)
            print(f"⚙️ 第 {attempt} 次失败，{round(delay, 1)} 秒后重试，新的 timeout={base_timeout/1000:.1f}s")
            time.sleep(delay)

    print("❌ Firecrawl 全部重试失败")
    return {"url": url, "status": "fail"}


# ========== 主流程（只抓商品页） ==========
def main():
    if not os.path.exists(EXCEL_FILE):
        print("❌ 未找到 Excel 文件。")
        return

    df = pd.read_excel(EXCEL_FILE, engine="openpyxl")
    prompts = df["Prompt"].dropna().tolist() if "Prompt" in df.columns else df.iloc[:, 1].dropna().tolist()
    all_logs = []

    for idx, prompt in enumerate(prompts, 1):
        print(f"\n========== 🔍 Prompt {idx}/{len(prompts)} ==========")
        prompt_name = re.sub(r"[^a-zA-Z0-9_()（）\u4e00-\u9fa5-]", "_", prompt[:50])
        prompt_dir = os.path.join(BASE_DIR, prompt_name)
        os.makedirs(prompt_dir, exist_ok=True)

        # ✅ Etsy 搜索 URL
        search_url = f"https://www.etsy.com/search?q={prompt.replace(' ', '+')}"
        print(f"\n🔍 Etsy 搜索：{search_url}")

        # 用 Firecrawl 抓取 HTML 但不保存文件，只用于提取链接
        try:
            result = firecrawl.scrape(
                search_url,
                formats=["html"],
                only_main_content=False,
                timeout=TIMEOUT
            )
            html = result.html or ""
            urls = re.findall(r'https://www\.etsy\.com/listing/[0-9]+/[^\s"\'<>]+', html)
            urls = list(dict.fromkeys(urls))[:MAX_URLS]
            print(f"🔗 提取到 {len(urls)} 个商品链接")

        except Exception as e:
            print(f"❌ Etsy 搜索抓取失败: {e}")
            continue

        if not urls:
            print("⚠️ 未提取到商品链接，跳过。")
            continue

        # ✅ 只抓取商品详情页
        for url in urls:
            result = fetch_with_firecrawl(url, prompt_dir)
            result.update({
                "prompt": prompt,
                "prompt_dir": prompt_dir,
                "search_url": search_url
            })
            all_logs.append(result)
            time.sleep(1)

    # 输出结果 CSV
    df_out = pd.DataFrame(all_logs)
    df_out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
    print(f"\n✅ 已保存结果：{OUTPUT_FILE}")
    print(f"📁 各 prompt 文件保存在：{BASE_DIR}")


if __name__ == "__main__":
    main()
