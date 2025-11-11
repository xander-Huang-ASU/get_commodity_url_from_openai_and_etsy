import os
import re
import time
import json
import csv
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import openai
from firecrawl import Firecrawl

# ========== ⚙️ 环境配置 ==========
# 在同目录下创建 .env 文件：
# OPENAI_API_KEY=sk-xxxx
# FIRECRAWL_API_KEY=fc-xxxx
load_dotenv()

OPENAI_API_KEY = ""
FIRECRAWL_API_KEY = ""

# ========== ⚙️ 常量配置 ==========
MODEL = "gpt-5"
EXCEL_FILE = "prompts.xlsx"
OUTPUT_FILE = "outputs_etsy/openai_firecrawl_summary.csv"
BASE_DIR = "outputs_etsy"
MAX_URLS = 3
MAX_RETRIES = 3
RETRY_DELAY = 3  # 秒
TIMEOUT = 300000    # Firecrawl超时（毫秒）

os.makedirs(BASE_DIR, exist_ok=True)
openai.api_key = OPENAI_API_KEY

# 初始化 Firecrawl SDK
firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)


# ========== Step 1️⃣ GPT 搜索 Etsy 网页 ==========
def get_urls_from_openai(prompt: str, model=MODEL, max_urls=MAX_URLS):
    print(f"\n🧠 调用 GPT WebSearch: {prompt[:60]}...")
    try:
        response = openai.responses.create(
            model=model,
            tools=[{"type": "web_search_preview", "search_context_size": "high"}],
            input=prompt + "\nReturn 5 product URLs with full https:// links."
        )

        text_output = ""
        for item in getattr(response, "output", []) or []:
            if getattr(item, "type", "") == "message":
                for content in getattr(item, "content", []) or []:
                    if getattr(content, "type", "") == "output_text":
                        text_output += getattr(content, "text", "")

        urls = re.findall(r"https?://[^\s)\"'>]+", text_output)
        urls = [u.rstrip(".,)]") for u in urls]
        print(f"🔗 提取到 {len(urls)} 个链接")
        return urls[:max_urls], text_output
    except Exception as e:
        print(f"❌ GPT 调用失败: {e}")
        return [], ""


# ========== Step 2️⃣ Firecrawl SDK 抓取 HTML + Markdown + 完整 JSON ==========
def fetch_with_firecrawl(url, prompt_dir):
    """
    使用 Firecrawl SDK 抓取 HTML + Markdown + 完整 JSON（空 schema）
    增强版：
    ✅ 动态延迟（防止触发限流）
    ✅ 智能重试（逐次延长等待时间）
    ✅ 自动延长 timeout
    ✅ 检测防爬页面（"Please enable JS"）
    """
    import random

    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", url[:80])
    md_path = os.path.join(prompt_dir, f"{safe_name}.md")
    html_path = os.path.join(prompt_dir, f"{safe_name}.html")
    json_path = os.path.join(prompt_dir, f"{safe_name}_full.json")

    base_timeout = TIMEOUT  # 初始超时（毫秒）
    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🕸️ Firecrawl 抓取第 {attempt}/{MAX_RETRIES} 次: {url}")
        try:
            # ========== 计时 ==========
            start = time.time()

            # ========== 执行抓取 ==========
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
                only_main_content=False,  # ✅ 只抓正文部分，提速显著
                timeout=base_timeout
            )

            elapsed = round(time.time() - start, 2)

            # ========== 检查是否被防爬 ==========
            if result.html and "Please enable JS" in result.html:
                print("⚠️ 检测到反爬页面（需要启用 JS），准备延迟后重试...")
                # 增加随机延迟后重试
                delay = random.uniform(10, 20)
                print(f"⏳ 等待 {round(delay, 1)} 秒再试...")
                time.sleep(delay)
                continue

            # ========== 保存文件 ==========
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

            # ========== 动态延迟 + Timeout 调整 ==========
            delay = RETRY_DELAY * attempt + random.uniform(2, 5)
            base_timeout = int(base_timeout * 1.5)  # 每次超时后自动延长 1.5 倍
            print(f"⚙️ 第 {attempt} 次失败，{round(delay, 1)} 秒后重试，"
                  f"新的 timeout={base_timeout/1000:.1f}s")

            time.sleep(delay)

    print("❌ Firecrawl 全部重试失败")
    return {"url": url, "status": "fail"}


# ========== Step 3️⃣ 主流程 ==========
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

        urls, gpt_output = get_urls_from_openai(prompt)
        if not urls:
            print("⚠️ GPT 未返回 URL，跳过。")
            continue

        for url in urls:
            result = fetch_with_firecrawl(url, prompt_dir)
            result.update({
                "prompt": prompt,
                "prompt_dir": prompt_dir,
                "gpt_raw_output": gpt_output
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
