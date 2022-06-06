import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
from aiohttp import client_exceptions
import json
import random


async def cek_data(ktp, session):
    url = (
        'http://smile.bpjsketenagakerjaan.go.id/smile/mod_kn/ajax/kn5004_'
        'grid_tk_aktif_query.php?{}'.format(random.random())
    )
    payload = {
        'TYPE': "getTK",
        'SEARCHA': "sc_kpj",
        'SEARCHB': ktp,
        'SEARCHC': "",
        'SEARCHD': "",
        'SEARCHE': "",
        'SEARCHF': "",
        'SEARCHG': "",
        'PAGE': 1,
    }
    count = 0
    while count <= 5:
        try:
            async with session.post(url, data=payload) as response:
                content = await response.text()
                data_tk = json.loads(content[71:])
                ret_data = [d for d in data_tk if d['KODE_NA'] is None]
                return ret_data
        except asyncio.exceptions.TimeoutError:
            count += 1
            print("%s - Timeouterror - count %d" % (ktp, count))
            await asyncio.sleep(5)
        except client_exceptions.ClientConnectorError:
            count += 1
            print("%s - ClientConnectorError -  count %d" % (ktp, count))
            await asyncio.sleep(5)
        except client_exceptions.ClientOSError:
            count += 1
            print("%s - ClientOSError - count %d" % (ktp, count))
            await asyncio.sleep(5)
        except json.decoder.JSONDecodeError:
            count += 1
            print("%s - JSONDecodeError - count %d" % (ktp, count))
            await asyncio.sleep(5)


def do_check(ktp, session):
    return cek_data(ktp, session)


async def safe_download(ktp, session):
    sem = asyncio.Semaphore(10)
    async with sem:
        return await do_check(ktp, session)


async def run(data):
    tasks = []
    async with cs() as session:
        for ktp in data["ktp"]:
            task = asyncio.ensure_future(safe_download(ktp, session))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results


def create_excel(datas):
    # with open("data.json", "w") as outfile:
    #   json.dump(dutk, outfile)
    data = []
    for tks in datas:
        for tk in tks:
            if tk:
                data.append(tk)
    df = pd.DataFrame(data)
    writer = pd.ExcelWriter(
        "cek-kpj-test.xlsx", engine="xlsxwriter",
        datetime_format='mm-yyyy'
    )
    df.to_excel(
        writer, sheet_name="Merged", index=False,
        columns=[
            "NOMOR_IDENTITAS", "KPJ", "NAMA_LENGKAP", "TGL_LAHIR", "NPP",
            "NAMAPRS", "TGL_KEPESERTAAN1", "TGL_NA", "NM_PRG"
        ]
    )
    writer.sheets["Merged"]
    writer.save()


if __name__ == '__main__':
    df = pd.read_excel(
        r"./cek-kpj.xlsx",
        0,
        converters={'ktp': str}
    )
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(df))
    results = loop.run_until_complete(future)
    create_excel(results)
