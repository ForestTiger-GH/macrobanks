# -*- coding: utf-8 -*-
"""
CBR Mortgage archiver
Скачивает выбранные Excel-файлы ЦБ РФ и упаковывает их в архив с именем YYYY-MM-DD.zip.
Работает в Jupyter и Google Colab (в Colab автоматически скачает готовый zip).
"""

import os
import re
import io
import sys
import shutil
import zipfile
import datetime as dt
from urllib.parse import urlparse
from typing import Iterable, Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Конфигурация по умолчанию: три ссылки ЦБ ---
DEFAULT_URLS = [
    "https://www.cbr.ru/vfs/statistics/banksector/mortgage/02_41_Mortgage_ihc.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_02_Mortgage.xlsx",
    "https://www.cbr.ru/vfs/statistics/BankSector/Mortgage/02_03_Scpa_mortgage.xlsx",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
)

def _requests_session() -> requests.Session:
    """Сессия с ретраями и нужными заголовками."""
    s = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
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
    """Пытается достать имя файла из Content-Disposition."""
    cd = resp.headers.get("Content-Disposition", "")
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
    Генерирует варианты URL с разным регистром каталогов BankSector/Mortgage.
    Оригинал — первым; далее — уникальные альтернативы.
    """
    variants = [url]
    for b in ("BankSector", "banksector"):
        for m in ("Mortgage", "mortgage"):
            v = re.sub(r"/[Bb]ank[Ss]ector/", f"/{b}/", url)
            v = re.sub(r"/[Mm]ortgage/", f"/{m}/", v)
            if v not in variants:
                variants.append(v)
    return variants

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
            if r.status_code == 200 and r.headers.get("Content-Type", "").lower().find("text/html") == -1:
                # Определяем имя
                fname = _filename_from_response(r) or _basename_from_url(u)
                if not fname:
                    fname = "downloaded_file"
                # Сохраняем потоково
                out_path = os.path.join(dest_dir, fname)
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                print(f"✅ Скачано: {fname}  ←  {u}")
                return out_path
            else:
                last_error = f"HTTP {r.status_code}"
        except Exception as e:
            last_error = str(e)

    print(f"❌ Не удалось скачать по адресу(ам): {url} (последняя ошибка: {last_error})")
    return None

def cbr_mortgage_archiver(
    urls: Optional[Iterable[str]] = None,
    archive_name: Optional[str] = None,
    work_dir: Optional[str] = None,
    auto_download_in_colab: bool = True,
) -> str:
    """
    Скачивает файлы ЦБ (ипотека) и упаковывает их в zip.
    :param urls: список URL; по умолчанию DEFAULT_URLS
    :param archive_name: имя архива без пути; по условию — 'YYYY-MM-DD.zip'.
    :param work_dir: рабочая папка для временных файлов; по умолчанию создается 'CBR_Mortgage_<date>'.
    :param auto_download_in_colab: в Colab автоматически скачать архив на машину пользователя.
    :return: полный путь к zip-файлу.
    """
    urls = list(urls) if urls is not None else list(DEFAULT_URLS)

    # Имя архива: только дата YYYY-MM-DD (по задаче)
    if archive_name is None:
        today = dt.date.today().strftime("%Y-%m-%d")
        archive_name = f"{today}.zip"

    # Рабочая директория
    if work_dir is None:
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        work_dir = f"CBR_Mortgage_{stamp}"
    os.makedirs(work_dir, exist_ok=True)

    session = _requests_session()

    saved_files: List[str] = []
    print("— Скачивание файлов ЦБ (ипотека)…")
    for url in urls:
        path = _download_one(session, url, work_dir)
        if path:
            saved_files.append(path)

    if not saved_files:
        raise RuntimeError("Не удалось скачать ни одного файла. Проверьте доступность ссылок/интернет.")

    # Упаковка в zip в текущей папке
    zip_path = os.path.abspath(archive_name)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for f in saved_files:
            zf.write(f, arcname=os.path.basename(f))
    print(f"📦 Готов архив: {zip_path}")

    # (Опционально) Чистим временную папку
    try:
        shutil.rmtree(work_dir)
    except Exception:
        pass

    # Автоскачивание в Colab (если есть)
    if auto_download_in_colab:
        try:
            from google.colab import files as gfiles  # type: ignore
            gfiles.download(zip_path)
        except Exception:
            # Не в Colab — просто оставим файл на диске
            pass

    return zip_path


# --- Пример использования (раскомментируйте для запуска сразу) ---
# cbr_mortgage_archiver()
