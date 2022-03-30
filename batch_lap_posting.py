import xml.etree.ElementTree as etree
import pandas as pd
import asyncio
from aiohttp import ClientSession as cs
from datetime import datetime, date
import aiohttp
import os
import aiofiles
import calendar


INDIR = "indir"
OUTDIR = "outdir"


URL = (
    "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setauth?butt"
    "on=Submit&username=smile&password=smilekanharimu&authtype=D&mask=GQ%253D%"
    "253D&isjsp=no&database=dboltp&nextpage=destype%3Dcache%26desformat%3DPDF%"
    "26report%3DKNR50504.rdf%26userid%3D%2Fdata%2Freports%2Fkn%26%26P_AWAL%3D'"
    "{awal}'%26P_AKHIR%3D'{akhir}'%26P_NPP%3D'{npp}'%26P_USER%3D'AK153580'"
)

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

async def download_dutk(data, session):
    url = URL.format(**data)
    loop = 0
    while loop < 10:
        try:
            async with session.get(url) as response:
                content = await response.read()
                if response.status != 200:
                    print(f"Download Failed: {response.status}")
                    return
                blth = datetime.strptime(data["awal"], "%d-%m-%Y").strftime("%y%m")
                dest_file = "{}_{}.pdf".format(data["npp"], blth)
                outdir = os.path.join(os.getcwd(), OUTDIR, blth)
                create_dir_ifn_exist(outdir)
                filename = os.path.join(outdir, dest_file)
                async with aiofiles.open(filename, "+wb") as f:
                    await f.write(content)
                return
        except asyncio.TimeoutError:
            print("TimeoutError: %s, loop: %s" % (data["npp"], loop))
            loop += 1
            await asyncio.sleep(5)
        except aiohttp.ClientError:
            print(
                "ServerDisconnectedError: %s, loop: %s" % (data["npp"], loop)
            )
            loop += 1
            await asyncio.sleep(5)
        except etree.ParseError:
            print("ParserError: %s, loop: %s" % (data["npp"], loop))
            loop += 1
            await asyncio.sleep(5)
    return None


def do_download(npp, session):
    return download_dutk(npp, session)


async def safe_download(npp, session):
    sem = asyncio.Semaphore(2)
    async with sem:
        return await do_download(npp, session)


async def run(data):
    tasks = []
    async with cs() as session:
        for npp in data:
            task = asyncio.ensure_future(safe_download(npp, session))
            tasks.append(task)
        return await asyncio.gather(*tasks)


def start_download(sheet):
    # variable npp dan periode
    input_file = "download_posting.xlsx"
    input_abs = os.path.join(os.getcwd(), INDIR, input_file)
    df = pd.read_excel(input_abs, 0)
    data_dict = df.to_dict('records')
    all_data = []
    for data in data_dict:
        month_list = pd.date_range(
            data["awal"], data["akhir"],
            freq='MS').strftime("%d-%m-%Y").tolist()
        for month in month_list:
            npp = dict()
            npp["npp"] = data["npp"]
            npp["awal"] = month
            hari, bulan, tahun = month.split("-")
            last_day = datetime.strptime(month, "%d-%m-%Y").replace(day=calendar.monthrange(int(tahun), int(bulan))[1])
            npp["akhir"] = last_day.strftime("%d-%m-%Y")
            all_data.append(npp)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(all_data))
    results = loop.run_until_complete(future)


if __name__ == '__main__':
    start_download(0)
