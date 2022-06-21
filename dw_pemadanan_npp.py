from aiohttp import ClientSession
import aiohttp.client_exceptions as ce
import csv
import asyncio
import pandas as pd


async def fetch(npp, session):
    url = (
        "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setauth?"
        "button=Submit&username=smile&password=smilekanharimu&authtype=D&mask"
        "=GQ%253D%253D&isjsp=no&database=dboltp&nextpage=destype%3Dcache%26des"
        "format%3DDELIMITEDDATA%26delimiter%3D|%26report%3DKNR3437.rdf%26useri"
        "d%3D%2Fdata%2Freports%2Fkn%26%26P_NPP%3D%27{}%27%26P_DIV%3D%27000%27%"
        "26P_USER%3D%27AK153580%27"
    ).format(npp)
    loop = 0
    while loop < 10:
        try:
            async with session.get(url) as response:
                content = await response.text()
                lines = content.splitlines()
                a = [
                    {
                        k: v for k, v in row.items()
                    } for row in csv.DictReader(
                        lines, delimiter="|", skipinitialspace=True
                    )
                ]
                return a
        except ce.ServerDisconnectedError:
            print("ServerDisconnectedError: " + npp + " - " + str(loop))
            loop += 1
            await asyncio.sleep(10)


async def safe_download(npp, session):
    sem = asyncio.Semaphore(3)
    async with sem:
        return await fetch(npp, session)


async def run(data):
    tasks = []
    async with ClientSession() as session:
        for npp in data:
            task = asyncio.ensure_future(
                safe_download(npp, session)
            )
            tasks.append(task)
        return await asyncio.gather(*tasks)


def safe_as_excel(dict_data):
    all_data = []
    for data in dict_data:
        all_data.extend(data)
    df = pd.DataFrame.from_dict(all_data)
    df.to_excel(
        './outdir/tk_invalid_all_baru_220610.xlsx',
        index=None,
        header=True,
        columns=[

        ]
    )


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    xls = pd.read_excel(
        r"D:\Script\dw_pemadanan_npp.xlsx"
    )["npp"].tolist()
    # xls = ["18010045"]
    future = asyncio.ensure_future(run(xls))
    all_data = loop.run_until_complete(future)
    safe_as_excel(all_data)
