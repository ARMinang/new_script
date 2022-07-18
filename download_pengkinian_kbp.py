import xml.etree.ElementTree as etree
import os
import requests
import argparse
from datetime import datetime
import time
import pandas as pd


OUTDIR = "outdir"
URL = (
    "http://rptserver.bpjsketenagakerjaan.go.id/reports/rwservlet/setau"
    "th?button=Submit&username=smile&password=smilekanharimu&authtype=D"
    "&mask=GQ%253D%253D&isjsp=no&database=dboltp&nextpage=destype%3Dcac"
    "he%26desformat%3DXML%26report%3DKNR4001.rdf%26userid%3D%2Fdata%2Fr"
    "eports%2Fkn%26%26P_PEMBINA%3D%27%27%26P_USER%3D%27{kbp}%27%26P_"
    "KANTOR_PEMBINA%3D%27%27%26P_KODE_KANTOR%3D%27D00%27%26P_KODE_SEGME"
    "N%3D%27PU%27%26P_TGL_PROSES%3D%27{periode}%27"
)

HEADER = [
    "NO", "NOMOR_IDENTITAS", "KPJ", "NPP", "KODE_DIVISI", "NAMA_PERUSAHAAN",
    "NAMA_LENGKAP", "TEMPAT_LAHIR", "TGL_LAHIR", "NAMA_IBU_KANDUNG",
    "BLTH_KEPESERTAAN", "EMAIL", "HP", "AKTIF", "BLTH_NA", "NO_REKENING",
    "NAMA_BANK", "NAMA_REKENING", "NAMA_SUMBER_DATA", "STATUS_VALID_IDENTITAS",
    "VALID_IBU_KANDUNG", "VALID_EMAIL", "VALID_HP", "VALID_NOREK"
]

DT_DATE = [
    "tgl_lahir", "blth_na", "blth_kepesertaan"
]

def create_dir_ifn_exist(path):
    is_exists = os.path.exists(path)
    if not is_exists:
        os.makedirs(path)

def download_pengkinian(data):
    url = URL.format(**data)
    data_tk = []
    count = 0
    while count <= 10:
        try:
            with requests.get(url) as resp:
                context = etree.fromstring(resp.text)
                for elem in context.iter("G_NAMA_KANTOR_WIL"):
                    pembina = elem.find("PEMBINA").text
                    for child in elem:
                        for list_tk in child.findall("G_NAMA_KANTOR"):
                            singleTk = dict(
                                (
                                    e.lower(),
                                    list_tk.find(e).text
                                    if list_tk.find(e) is not None else ""
                                )
                                for e in HEADER
                            )
                            singleTk["pembina"] = pembina
                            data_tk.append(singleTk)
                return data_tk
        except requests.exceptions.ConnectionError as ce:
            print(ce)


def create_excel(datas, outname):
    data = []
    data_invalid = []
    for tk in datas:
        if tk:
            if tk["status_valid_identitas"] == "T":
                data_invalid.append(tk)
            data.append(tk)
    df = pd.DataFrame(
        data,
        columns=list(map(str.lower, HEADER)).append("pembina")
    )
    
    for dtime in DT_DATE:
        df[dtime] = pd.to_datetime(df[dtime], format="%d-%b-%y")
    with pd.ExcelWriter(
        outname,
        engine="xlsxwriter",
        datetime_format='dd-mm-yyyy',
    ) as writer:
        df.to_excel(
            writer, sheet_name="belum pengkinian",
            index=False
        )
        if data_invalid:
            dfi = pd.DataFrame(
                data_invalid,
                columns=list(map(str.lower, HEADER)).append("pembina")
            )
            for dtime in DT_DATE:
                dfi[dtime] = pd.to_datetime(dfi[dtime], format="%d-%b-%y")
            dfi.to_excel(
                writer, sheet_name="invalid",
                index=False
            )


if __name__ == "__main__":
    outdir = os.path.join(os.getcwd(), OUTDIR)
    create_dir_ifn_exist(outdir)
    outname = os.path.join(
        outdir,
        "PENGKINIAN_{}.xlsx".format(datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
    )
    parser = argparse.ArgumentParser(description='Script untuk ambil data dari Portal Adminduk')
    parser.add_argument(
        '--outfile', type=str, help='output file xlsx', default=outname, action="store"
    )
    args = parser.parse_args()
    start_time = time.time()
    results = download_pengkinian(
        {
            "kbp": "RU138430",
            "periode": datetime.today().strftime("%d-%m-%Y")
        }
    )
    create_excel(results, args.outfile)
    print("--- %s seconds ---" % (time.time() - start_time))
