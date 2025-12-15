import os
import re
import time
import json
import random
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from firecrawl import Firecrawl
from bs4 import BeautifulSoup
import openai

from pydantic import BaseModel, Field
from typing import Optional, List

# ================================================================
# 环境 & 全局配置
# ================================================================
load_dotenv()

FIRECRAWL_API_KEY = ""
OPENAI_API_KEY = ""

if not FIRECRAWL_API_KEY:
    raise ValueError("❌ 请在环境变量中设置 FIRECRAWL_API_KEY")
if not OPENAI_API_KEY:
    print("⚠️ 未检测到 OPENAI_API_KEY，只能运行 SEO 爬取（GEO 会跳过）")

openai.api_key = OPENAI_API_KEY

# Firecrawl 客户端
firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)

# 目录 / 文件配置
EXCEL_FILE = "prompts.xlsx"
SEO_BASE_DIR = "outputs_etsy_final"      # 对应任务中的 SEO 目录
GEO_BASE_DIR = "outputs_GEO_final"     # 对应任务中的 GEO 目录

os.makedirs(SEO_BASE_DIR, exist_ok=True)
os.makedirs(GEO_BASE_DIR, exist_ok=True)

# 抓取参数
MAX_LISTINGS_PER_QUERY = 10      # 任务要求：每个 query × channel 3 个结果 → 50*3*2=300
MAX_RETRIES = 3
RETRY_DELAY = 3   # 秒
TIMEOUT = 300000  # ms，Firecrawl timeout

# OpenAI 模型（你可以按自己环境改）
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5")


# ================================================================
# Pydantic Schema：用于 Firecrawl JSON 结构化输出
# ================================================================
class EtsyImage(BaseModel):
    """描述单个商品图片及其元数据"""
    url: Optional[str] = Field(None, description="The full URL of the product image.")
    alt_text: Optional[str] = Field(None, description="The image's alt text for SEO and accessibility.")


class EtsyProduct(BaseModel):
    """Etsy 商品页面的结构化数据模型"""

    # A. Basic product attributes
    title: Optional[str] = None
    price: Optional[str] = None
    originalPrice: Optional[str] = None
    rating: Optional[str] = None
    reviewsCount: Optional[str] = None
    bestseller: Optional[bool] = None
    starSeller: Optional[bool] = None

    # B. Shop metadata
    shop: Optional[str] = None
    delivery: Optional[str] = None
    shopPolicies: Optional[str] = None
    purchaseProtection: Optional[str] = None
    return_policy_text: Optional[str] = None
    paymentMethods: Optional[List[str]] = None

    # C. Images
    images: Optional[List[EtsyImage]] = None

    # D. Description
    description_text: Optional[str] = None

    # E. FAQ
    faq_items: Optional[List[str]] = None


# ================================================================
# 工具函数：安全命名
# ================================================================
def safe_query_name(prompt: str) -> str:
    """
    用于生成 query 子目录名：
    - 保留字母、数字、下划线、部分括号和中文
    - 其他字符替换为 "_"
    """
    return re.sub(r"[^a-zA-Z0-9_()（）\u4e00-\u9fa5-]", "_", prompt[:50])


def safe_slug_from_url(url: str, max_len: int = 80) -> str:
    """
    根据 URL 生成文件名 slug，避免特殊字符。
    """
    return re.sub(r"[^a-zA-Z0-9_-]", "_", url[:max_len])


# ================================================================
# SEO 渠道：直接用 Etsy 搜索页面提取 listing URL
# ================================================================
def get_seo_urls_from_etsy(prompt: str, max_urls: int = MAX_LISTINGS_PER_QUERY):
    query_param = prompt.replace(" ", "+")
    search_url = f"https://www.etsy.com/search?q={query_param}"
    print(f"\n🔍 [SEO] Etsy 搜索 URL: {search_url}")

    urls = []
    try:
        result = firecrawl.scrape(
            search_url,
            formats=["html"],
            only_main_content=False,
            timeout=TIMEOUT
        )
        html = result.html or ""
        # 提取 listing URL
        urls = re.findall(r'https://www\.etsy\.com/listing/[0-9]+/[^\s"\'<>]+', html)
        # 去重，保留前 max_urls 个
        urls = list(dict.fromkeys(urls))[:max_urls]
        print(f"🔗 [SEO] 提取到 {len(urls)} 个商品链接")
    except Exception as e:
        print(f"❌ [SEO] Etsy 搜索抓取失败: {e}")

    return urls, search_url


# ================================================================
# GEO 渠道：使用 OpenAI WebSearch 获取 Etsy listing URL
# ================================================================
def get_geo_urls_from_openai(prompt: str, max_urls: int = MAX_LISTINGS_PER_QUERY):
    if not OPENAI_API_KEY:
        print("⚠️ 未配置 OPENAI_API_KEY，跳过 GEO URL 获取")
        return [], ""

    full_prompt = (
        prompt
        + "\nSearch the web and return ONLY a list of Etsy product URLs."
          "\nDo NOT return JSON."
          "\nDo NOT return structured data."
          "\nReturn one URL per line, each starting with https://."
    )

    print(f"\n🧠 [GEO] GPT WebSearch for prompt: {prompt[:60]}")

    raw_text = ""
    urls = []

    try:
        # 这里沿用你之前的 Responses API 调用风格
        response = openai.responses.create(
            model=OPENAI_MODEL,
            tools=[{"type": "web_search_preview"}],
            input=full_prompt,
        )

        # 解析文本输出部分（根据你之前的代码风格）
        for item in response.output:
            if item.type == "message":
                for block in item.content:
                    if block.type == "output_text":
                        raw_text += block.text

        # 从 raw_text 中提取 Etsy listing URL
        urls = [
            u for u in re.findall(r"https?://[^\s\"'>]+", raw_text)
            if "etsy.com" in u and "/listing/" in u
        ]
        urls = list(dict.fromkeys(urls))[:max_urls]

        print(f"🔗 [GEO] GPT 返回 {len(urls)} 个商品链接")
    except Exception as e:
        print(f"❌ [GEO] GPT WebSearch 出错：{e}")

    return urls, raw_text


# ================================================================
# Firecrawl 单页抓取：Markdown / HTML / JSON
# ================================================================
def fetch_with_firecrawl(url: str, out_dir: str, channel: str, query_id: str, rank: int):
    """
    对单个商品页进行抓取，保存 md / html / json。
    返回一条日志 dict。
    """
    os.makedirs(out_dir, exist_ok=True)
    slug = safe_slug_from_url(url)
    md_path = os.path.join(out_dir, f"{slug}.md")
    html_path = os.path.join(out_dir, f"{slug}.html")
    json_path = os.path.join(out_dir, f"{slug}_full.json")

    timeout = TIMEOUT
    base_timeout = timeout

    for attempt in range(1, MAX_RETRIES + 1):
        print(f"\n🕸️ [{channel}] Firecrawl 抓取 {attempt}/{MAX_RETRIES}: {url}")
        start_time = time.time()

        try:
            result = firecrawl.scrape(
                url,
                formats=[
                    "markdown",
                    "html",
                    {
                        "type": "json",
                        "schema": EtsyProduct.model_json_schema()
                    }
                ],
                only_main_content=False,
                timeout=timeout
            )

            elapsed = round(time.time() - start_time, 2)

            # 反爬检测示例：如果看到需要启用 JS 的提示，就 retry
            if result.html and "Please enable JS" in result.html:
                print("⚠️ 检测到可能的反爬页面，等待后重试...")
                delay = random.uniform(10, 20)
                print(f"⏳ 等待 {round(delay, 1)} 秒...")
                time.sleep(delay)
                continue

            # 保存文件
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(result.markdown or "")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(result.html or "")
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(result.json or {}, f, ensure_ascii=False, indent=2)

            print(f"✅ [{channel}] 抓取成功 ({elapsed}s) → {slug}")
            return {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "channel": channel,
                "query_id": query_id,
                "rank": rank,
                "url": url,
                "url_slug": slug,
                "status": "success",
                "md_path": md_path,
                "html_path": html_path,
                "json_path": json_path,
                "attempt": attempt,
                "elapsed_s": elapsed,
            }

        except Exception as e:
            elapsed = round(time.time() - start_time, 2)
            print(f"❌ [{channel}] Firecrawl 抓取失败 (第 {attempt} 次) [{elapsed}s]: {e}")
            # 线性 + 随机延时，timeout 递增
            delay = RETRY_DELAY * attempt + random.uniform(1, 3)
            timeout = int(timeout * 1.5)
            print(f"⚙️ 等待 {round(delay, 1)} 秒后重试，新的 timeout = {timeout/1000:.1f} 秒")
            time.sleep(delay)

    print(f"🛑 [{channel}] Firecrawl 多次重试仍失败: {url}")
    return {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "channel": channel,
        "query_id": query_id,
        "rank": rank,
        "url": url,
        "status": "fail"
    }


# ================================================================
# 主流程：同时跑 SEO + GEO 爬取
# ================================================================
def load_prompts_from_excel(excel_file: str):
    if not os.path.exists(excel_file):
        print(f"❌ 找不到 Excel 文件：{excel_file}")
        return []

    try:
        df = pd.read_excel(excel_file, engine="openpyxl")
    except Exception:
        df = pd.read_excel(excel_file)  # 兜底

    # 优先找名为 "Prompt" 的列，否则默认用第二列
    if "Prompt" in df.columns:
        prompts = df["Prompt"].dropna().tolist()
    else:
        prompts = df.iloc[:, 1].dropna().tolist()

    print(f"📑 从 {excel_file} 加载到 {len(prompts)} 条 Prompt")
    return prompts


def run_crawling():
    prompts = load_prompts_from_excel(EXCEL_FILE)
    if not prompts:
        return

    seo_logs = []
    geo_logs = []

    for idx, prompt in enumerate(prompts, 1):
        print("\n" + "=" * 70)
        print(f"🎯 Query {idx}/{len(prompts)}: {prompt}")
        print("=" * 70)

        query_id = safe_query_name(prompt)

        # ------------------------------------------------------------------
        # 1) SEO 渠道：Etsy 搜索
        # ------------------------------------------------------------------
        seo_query_dir = os.path.join(SEO_BASE_DIR, query_id)
        os.makedirs(seo_query_dir, exist_ok=True)

        seo_urls, seo_search_url = get_seo_urls_from_etsy(prompt)
        if not seo_urls:
            print(f"⚠️ [SEO] 未提取到商品链接，跳过该 query 的 SEO 部分")
        else:
            for rank, url in enumerate(seo_urls, 1):
                if rank > MAX_LISTINGS_PER_QUERY:
                    break
                log_row = fetch_with_firecrawl(
                    url=url,
                    out_dir=seo_query_dir,
                    channel="SEO",
                    query_id=query_id,
                    rank=rank
                )
                log_row["prompt"] = prompt
                log_row["search_url"] = seo_search_url
                seo_logs.append(log_row)
                # 礼貌性等待，避免过快请求
                time.sleep(1)

        # ------------------------------------------------------------------
        # 2) GEO 渠道：OpenAI WebSearch
        # ------------------------------------------------------------------
        geo_query_dir = os.path.join(GEO_BASE_DIR, query_id)
        os.makedirs(geo_query_dir, exist_ok=True)

        geo_urls, geo_raw_text = get_geo_urls_from_openai(prompt)
        if not geo_urls:
            print(f"⚠️ [GEO] 未获得 GPT 返回的商品链接，跳过该 query 的 GEO 部分")
        else:
            # 可选：保存 GPT 原始输出，方便分析
            raw_out_path = os.path.join(geo_query_dir, "gpt_raw_output.txt")
            try:
                with open(raw_out_path, "w", encoding="utf-8") as f:
                    f.write("PROMPT:\n" + prompt + "\n\n")
                    f.write("GPT RAW OUTPUT:\n\n" + geo_raw_text)
                print(f"📝 [GEO] 已保存 GPT 原始输出到 {raw_out_path}")
            except Exception as e:
                print(f"⚠️ [GEO] 保存 GPT 原始输出失败: {e}")

            for rank, url in enumerate(geo_urls, 1):
                if rank > MAX_LISTINGS_PER_QUERY:
                    break
                log_row = fetch_with_firecrawl(
                    url=url,
                    out_dir=geo_query_dir,
                    channel="GEO",
                    query_id=query_id,
                    rank=rank
                )
                log_row["prompt"] = prompt
                geo_logs.append(log_row)
                time.sleep(1)

    # ================================================================
    # 保存两个渠道的抓取日志
    # ================================================================
    if seo_logs:
        df_seo = pd.DataFrame(seo_logs)
        seo_log_path = os.path.join(SEO_BASE_DIR, "seo_crawl_summary.csv")
        df_seo.to_csv(seo_log_path, index=False, encoding="utf-8-sig")
        print(f"\n✅ [SEO] 抓取日志已保存: {seo_log_path}")

    if geo_logs:
        df_geo = pd.DataFrame(geo_logs)
        geo_log_path = os.path.join(GEO_BASE_DIR, "geo_crawl_summary.csv")
        df_geo.to_csv(geo_log_path, index=False, encoding="utf-8-sig")
        print(f"✅ [GEO] 抓取日志已保存: {geo_log_path}")

    print("\n🎉 爬取流程完成（SEO + GEO）")
    print(f"   SEO 条数: {len(seo_logs)}")
    print(f"   GEO 条数: {len(geo_logs)}")


# ================================================================
# 脚本入口
# ================================================================
if __name__ == "__main__":
    run_crawling()
