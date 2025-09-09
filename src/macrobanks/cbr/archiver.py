# -*- coding: utf-8 -*-
"""
CBR timeseries archiver
Скачивает выбранные Excel-файлы с сайта ЦБ РФ (полные временные ряды, не ежедневные снапшоты)
и упаковывает их в архив с именем YYYY-MM-DD.zip.
Поддерживает Jupyter и Google Colab (в Colab — авто-скачивание архива).
"""

import os
import re
import shutil
import zipfile
import datetime as dt
from typing import Iterable, Optional, List
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Ссылки по умолчанию: таймсерии ЦБ (ипотека + кредиты корпорациям) ---
DEFAULT_URLS = [
    # Кредиты физлиц
    "https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_05_Debt_ind.xlsx",
    # Ипотека (полные ряды)
    "https://www.cbr.ru/vfs/statistics/banksector/mortgage/02_41_Mortgage_ihc.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_02_Mortgage.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_03_Scpa_mortgage.xlsx",
    # Кредиты корпорациям (полные ряды)
    "https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_01_A_New_loans_corp_by_activity.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_01_C_New_loans_corp_by_activity.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_02_A_Debt_corp_by_activity.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_02_C_Debt_corp_by_activity.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_11_Debt_sme.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Loans_to_corporations/01_11_F_Debt_sme_by_activity.xlsx",
    # Долговые бумаги
    "https://www.cbr.ru/vfs/statistics/debt_securities/66-debt_securities.xlsx", 
    # Средства совокупные
    "https://www.cbr.ru/vfs/statistics/BankSector/Borrowings/02_01_Funds_all.xlsx", 
    "https://www.cbr.ru/vfs/statistics/banksector/borrowings/02_29_Budget_all.xlsx", 
    # Домашние хозяйства
    "https://cbr.ru/vfs/statistics/households/households_bm.xlsx",
    "https://cbr.ru/vfs/statistics/households/households_om.xlsx",
    # Уникальный файл с добавлением "_new"
    "https://www.cbr.ru/Content/Document/File/115862/obs_tabl20%D1%81.xlsx",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
)

def _requests_session() -> requests.Session:
    """Сессия с ретраями и вежливыми заголовками."""
    s = requests.Session()
    retries = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT})
    return s

def _filename_from_response(resp: requests.Response) -> Optional[str]:
    """Имя файла из Content-Disposition (если сервер дал подсказку)."""
    cd = resp.headers.get("Content-Disposition", "") or resp.headers.get("content-disposition", "")
    if not cd:
        return None
    m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
    if m:
        return os.path.basename(m.group(1))
    return None

def _basename_from_url(u: str) -> str:
    """Имя файла из URL (последний сегмент пути)."""
    return os.path.basename(urlparse(u).path)

def _alt_variants(url: str) -> List[str]:
    """
    Генерирует варианты URL с разным регистром ключевых каталогов:
    BankSector/banksector, Mortgage/mortgage, Loans_to_corporations/loans_to_corporations.
    Оригинал — первым.
    """
    variants = []
    for b in ("BankSector", "banksector"):
        for m in ("Mortgage", "mortgage"):
            for l in ("Loans_to_corporations", "loans_to_corporations"):
                v = url
                v = re.sub(r"/[Bb]ank[Ss]ector/", f"/{b}/", v)
                v = re.sub(r"/[Mm]ortgage/", f"/{m}/", v)
                v = re.sub(r"/[Ll]oans_to_corporations/", f"/{l}/", v)
                if v not in variants:
                    variants.append(v)
    # Гарантируем, что оригинальный URL стоит первым
    if url in variants:
        variants.remove(url)
    return [url] + variants

def _download_one(session: requests.Session, url: str, dest_dir: str) -> Optional[str]:
    """
    Скачивает один файл, пробуя оригинальный URL и альтернативные регистры.
    Возвращает путь к сохраненному файлу или None при неуспехе.
    """
    candidates = _alt_variants(url)
    last_error = None

    for u in candidates:
        try:
            r = session.get(u, timeout=30, stream=True, allow_redirects=True)
            ctype = (r.headers.get("Content-Type") or "").lower()
            if r.status_code == 200 and "text/html" not in ctype:
                fname = _filename_from_response(r) or _basename_from_url(u) or "downloaded_file"

                # Уникальное правило для obs_tabl20с.xlsx
                if "obs_tabl20c" in fname.lower():
                    base, ext = os.path.splitext(fname)
                    fname = f"{base}new{ext}"

                out_path = os.path.join(dest_dir, fname)
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                print(f"✅ Скачано: {fname}  ←  {u}")
                return out_path
            else:
                last_error = f"HTTP {r.status_code}, Content-Type: {ctype or 'unknown'}"
        except Exception as e:
            last_error = str(e)

    print(f"❌ Не удалось скачать: {url} (последняя ошибка: {last_error})")
    return None


def cbr_timeseries_archiver(
    urls: Optional[Iterable[str]] = None,
    archive_name: Optional[str] = None,
    work_dir: Optional[str] = None,
    auto_download_in_colab: bool = True,
) -> str:
    """
    Скачивает файлы ЦБ (полные временные ряды) и упаковывает их в zip.
    :param urls: список URL; по умолчанию DEFAULT_URLS.
    :param archive_name: имя архива; по условию — 'YYYY-MM-DD.zip'.
    :param work_dir: временная папка для загрузок; по умолчанию создается автоматически.
    :param auto_download_in_colab: в Colab автоматически скачать архив на компьютер.
    :return: абсолютный путь к zip-файлу.
    """
    urls = list(urls) if urls is not None else list(DEFAULT_URLS)

    # Имя архива строго по текущей дате
    if archive_name is None:
        archive_name = f"{dt.date.today():%Y-%m-%d}.zip"
        archive_name = "CBR Collected Series Files.zip"

    # Рабочая директория
    if work_dir is None:
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        work_dir = f"CBR_DL_{stamp}"
    os.makedirs(work_dir, exist_ok=True)

    session = _requests_session()

    saved_files: List[str] = []
    print("— Скачивание файлов ЦБ (таймсерии)…")
    for url in urls:
        path = _download_one(session, url, work_dir)
        if path:
            saved_files.append(path)

    if not saved_files:
        raise RuntimeError("Не удалось скачать ни одного файла. Проверьте ссылки и доступ в интернет.")

    # Упаковка в zip рядом с рабочей директорией
    zip_path = os.path.abspath(archive_name)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in saved_files:
            zf.write(f, arcname=os.path.basename(f))
    print(f"📦 Готов архив: {zip_path}")

    # Чистим временную папку
    try:
        shutil.rmtree(work_dir)
    except Exception:
        pass

    # Авто-скачивание в Colab
    if auto_download_in_colab:
        try:
            from google.colab import files as gfiles  # type: ignore
            gfiles.download(zip_path)
        except Exception:
            pass  # Не Colab — просто оставим файл на диске

    return zip_path


# --- Пример запуска ---
# cbr_timeseries_archiver()
