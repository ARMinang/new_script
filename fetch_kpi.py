import pandas as pd
import asyncio
import aiohttp
from aiohttp import ClientSession as cs
import os
from bs4 import BeautifulSoup
import random
from datetime import datetime
import unicodedata
from google.oauth2 import service_account
import gspread


INDIR = "indir"
OUTDIR = "outdir"
KEYS = ["Kode", "Indikator", "Bobot", "Target", "Realisasi", "Pencapaian", "Nilai", "Skor", "Status", "Trend", "Variance", "KodeKantor", "Timestamp"]
DT_INTEGER = [
    "Bobot", "Target", "Realisasi", "Pencapaian", "Nilai", "Skor"
]

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

async def get_tk_data(input_data, session):
    url = "http://smile.bpjsketenagakerjaan.go.id/smile/mod_kpi/ajax/kpi001_query.php?{}"
    payload = {
        "TASK": "",
        "TIPE": "sc_kode_kantor",
        "SEARCHA": input_data["kode_kantor"],
        "PAGE": "1",
        "PERIODE": input_data["tanggal"],
        "FORM": "FORM1"
    }
    count = 0
    while count < 5:
        try:
            async with session.post(url.format(random.random()), data=payload) as resp:
                content = await resp.text()
                soup = BeautifulSoup(content, 'html.parser')
                ids = soup.findAll('tr')
                all_data = []
                for id in ids:
                    texts = [unicodedata.normalize("NFKD", x.text).strip() for x in id.find_all("td")]
                    if len(texts) > 6 and texts[0] != "" and texts[5] != "":
                        texts.append(input_data["kode_kantor"])
                        texts.append(datetime.now())
                        dictionary = dict(zip(KEYS, texts))
                        all_data.append(dictionary)
                return all_data
        except aiohttp.client_exceptions.ClientOSError:
            print("ClientOSError")
            count += 1
            await asyncio.sleep(5)
        except asyncio.exceptions.TimeoutError:
            print("Asyncio TimeoutError")
            count += 1
            await asyncio.sleep(5)
        except IndexError:
            ret_data = dict()
            ret_data["MSG"] = "Data Tidak ditemukan"
            return ret_data


async def safe_download(data, session):
    sem = asyncio.Semaphore(10)
    async with sem:
        return await get_tk_data(data, session)


async def run(data):
    tasks = []
    async with cs() as session:
        for single in data:
            task = asyncio.ensure_future(safe_download(single, session))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results

def create_credentials():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = service_account.Credentials.from_service_account_file(
        "./credentials.json",
        scopes=scopes
    )
    return gspread.authorize(credentials)


def create_excel(data):
    final_data = []
    for entries in data:
        for entry in entries:
            final_data.append(entry)
    df = pd.DataFrame(
        final_data,
        columns=KEYS
    )
    for num in DT_INTEGER:
        df[num] = df[num].str.replace(".", "")
        df[num] = pd.to_numeric(df[num].str.replace(",", "."), downcast="float")
    df["Variance"] = pd.to_numeric(df[num], downcast="float")
    df.round(2)
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    save_filename = os.path.join(
        outdir,
        "KPI_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    with pd.ExcelWriter(
        save_filename,
        engine="xlsxwriter",
        datetime_format='dd-mm-yyyy',
    ) as writer:
        df.to_excel(
            writer, sheet_name="Lol",
            index=False
        )


if __name__ == "__main__":
    # variable npp dan periode
    kode_kantors = [
        "G00", "D02", "P00", "N11", "L00", "L01", "K05",
        "K10", "D00", "B00", "L03", "N00", "J04", "X00",
        "K04","K00","K02","K01","K13","K06"
    ]
    today = datetime.today().strftime("%Y%m")
    all_data = []
    for kode_kantor in kode_kantors:
        data_dict = dict()
        data_dict["kode_kantor"] = kode_kantor
        data_dict["tanggal"] = today
        all_data.append(data_dict)
    result = asyncio.run(run(all_data))
    create_excel(result)
