"""
Generate a 30-record Excel file for Heart360 that populates ALL dashboard graphs,
especially the 3 overdue graphs:
  1. Overdue at Start of Month
  2. Overdue Patients Called
  3. Overdue Returned to Care

Data design:
- 30 patients across 2 districts, 2 facilities (puskesmas), 4 SHCs
- Registration dates spread from Jul 2025 to Jan 2026 (> 3 months ago for "under care")
- Encounter (visit) dates vary:
    * Some recent (within 45 days) → NOT overdue
    * Some 46-90 days ago → overdue (shows in overdue list + start-of-month graph)
    * Some 91-180 days ago → overdue + lost to follow-up candidates
- BP values: mix of controlled (<140/90) and uncontrolled (>=140/90)
- Blood sugar: some FBS, RBS, HBA1C values
- Scheduled visits: for many patients
- Call results: for overdue patients (drives "patients called" graph)
    * Some with "Agreed to visit" who then have a follow-up encounter → "returned to care"
    * Some with other call results
- A few patients with no visit in past 3 months → "no visit" graph
"""

import openpyxl
from datetime import datetime, timedelta
import os

wb = openpyxl.Workbook()
ws = wb.active
ws.title = "Sheet1"

headers = [
    "puskesmas", "district", "shc", "nik", "no_rm_lama", "nama_pasien",
    "tgl_lahir", "jenis_kelamin", "no_telp", "tanggal_pendaftaran",
    "kunjungan_terakhir", "sistole", "diastole", "tgl_terjadwal",
    "tgl_panggilan", "jenis_hasil", "alasan_dihapus",
    "gula_darah", "jenis_gula_darah", "wilayah"
]
ws.append(headers)

TODAY = datetime(2026, 4, 7)
FMT = "%d-%m-%Y"

def d(dt):
    """Format datetime to DD-MM-YYYY string"""
    return dt.strftime(FMT) if dt else None

# Helper to make dates relative to today
def ago(days):
    return TODAY - timedelta(days=days)

records = [
    # === GROUP 1: CONTROLLED BP, recent visits (not overdue) ===
    # These populate: "patients protected", "BP controlled rate"
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2001, "nama": "Andi Pratama", "lahir": d(datetime(1970, 3, 15)),
        "jk": "Male", "telp": "081200001", "reg": d(ago(365)),
        "visit": d(ago(10)), "sys": 125, "dia": 78, "jadwal": d(ago(-30)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 95, "jenis_gula": "FBS",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2002, "nama": "Budi Santoso", "lahir": d(datetime(1965, 7, 22)),
        "jk": "Male", "telp": "081200002", "reg": d(ago(300)),
        "visit": d(ago(15)), "sys": 130, "dia": 82, "jadwal": d(ago(-25)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 110, "jenis_gula": "FBS",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2003, "nama": "Citra Dewi", "lahir": d(datetime(1980, 11, 5)),
        "jk": "Female", "telp": "081200003", "reg": d(ago(270)),
        "visit": d(ago(20)), "sys": 118, "dia": 75, "jadwal": d(ago(-20)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 5.4, "jenis_gula": "HBA1C",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Beta",
        "nik": 2004, "nama": "Dian Purnama", "lahir": d(datetime(1972, 5, 18)),
        "jk": "Female", "telp": "081200004", "reg": d(ago(330)),
        "visit": d(ago(25)), "sys": 135, "dia": 85, "jadwal": d(ago(-15)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 120, "jenis_gula": "RBS",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Beta",
        "nik": 2005, "nama": "Eko Wijaya", "lahir": d(datetime(1968, 9, 30)),
        "jk": "Male", "telp": "081200005", "reg": d(ago(250)),
        "visit": d(ago(30)), "sys": 128, "dia": 80, "jadwal": d(ago(-10)),
        "call": None, "hasil": None, "hapus": None,
        "gula": None, "jenis_gula": None,
    },

    # === GROUP 2: UNCONTROLLED BP, recent visits (not overdue) ===
    # These populate: "BP uncontrolled rate"
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2006, "nama": "Fitri Handayani", "lahir": d(datetime(1960, 1, 12)),
        "jk": "Female", "telp": "081200006", "reg": d(ago(350)),
        "visit": d(ago(12)), "sys": 155, "dia": 98, "jadwal": d(ago(-20)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 145, "jenis_gula": "FBS",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2007, "nama": "Gunawan Hadi", "lahir": d(datetime(1958, 4, 25)),
        "jk": "Male", "telp": "081200007", "reg": d(ago(320)),
        "visit": d(ago(18)), "sys": 162, "dia": 102, "jadwal": d(ago(-15)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 210, "jenis_gula": "RBS",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2008, "nama": "Hesti Rahayu", "lahir": d(datetime(1975, 8, 8)),
        "jk": "Female", "telp": "081200008", "reg": d(ago(280)),
        "visit": d(ago(22)), "sys": 148, "dia": 95, "jadwal": d(ago(-10)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 7.8, "jenis_gula": "HBA1C",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Delta",
        "nik": 2009, "nama": "Irfan Maulana", "lahir": d(datetime(1963, 12, 3)),
        "jk": "Male", "telp": "081200009", "reg": d(ago(310)),
        "visit": d(ago(35)), "sys": 170, "dia": 108, "jadwal": d(ago(-5)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 180, "jenis_gula": "RBS",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Delta",
        "nik": 2010, "nama": "Joko Susilo", "lahir": d(datetime(1955, 6, 20)),
        "jk": "Male", "telp": "081200010", "reg": d(ago(290)),
        "visit": d(ago(40)), "sys": 145, "dia": 92, "jadwal": d(ago(-3)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 135, "jenis_gula": "FBS",
    },

    # === GROUP 3: OVERDUE patients (last visit > 45 days ago) WITH CALL RESULTS ===
    # These populate: overdue patient list, overdue start of month, overdue patients called
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2011, "nama": "Kartini Sari", "lahir": d(datetime(1966, 2, 14)),
        "jk": "Female", "telp": "081200011", "reg": d(ago(400)),
        "visit": d(ago(60)), "sys": 150, "dia": 96, "jadwal": d(ago(10)),
        "call": d(ago(5)), "hasil": "Agreed to visit", "hapus": None,
        "gula": 160, "jenis_gula": "RBS",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2012, "nama": "Lukman Hakim", "lahir": d(datetime(1959, 10, 7)),
        "jk": "Male", "telp": "081200012", "reg": d(ago(380)),
        "visit": d(ago(75)), "sys": 158, "dia": 100, "jadwal": d(ago(20)),
        "call": d(ago(8)), "hasil": "Call again later", "hapus": None,
        "gula": None, "jenis_gula": None,
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Beta",
        "nik": 2013, "nama": "Maya Anggraini", "lahir": d(datetime(1978, 3, 28)),
        "jk": "Female", "telp": "081200013", "reg": d(ago(360)),
        "visit": d(ago(55)), "sys": 142, "dia": 91, "jadwal": d(ago(5)),
        "call": d(ago(3)), "hasil": "Agreed to visit", "hapus": None,
        "gula": 6.9, "jenis_gula": "HBA1C",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Beta",
        "nik": 2014, "nama": "Nur Hidayat", "lahir": d(datetime(1962, 7, 16)),
        "jk": "Male", "telp": "081200014", "reg": d(ago(340)),
        "visit": d(ago(90)), "sys": 165, "dia": 105, "jadwal": d(ago(30)),
        "call": d(ago(12)), "hasil": "Wrong phone number", "hapus": "Wrong phone number",
        "gula": 190, "jenis_gula": "RBS",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2015, "nama": "Okta Permana", "lahir": d(datetime(1971, 5, 2)),
        "jk": "Male", "telp": "081200015", "reg": d(ago(370)),
        "visit": d(ago(65)), "sys": 152, "dia": 94, "jadwal": d(ago(15)),
        "call": d(ago(7)), "hasil": "Agreed to visit", "hapus": None,
        "gula": 155, "jenis_gula": "FBS",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2016, "nama": "Putri Wulandari", "lahir": d(datetime(1983, 9, 11)),
        "jk": "Female", "telp": "081200016", "reg": d(ago(350)),
        "visit": d(ago(80)), "sys": 160, "dia": 99, "jadwal": d(ago(25)),
        "call": d(ago(10)), "hasil": "Multiple failed contact attempts",
        "hapus": "Multiple failed contact attempts",
        "gula": None, "jenis_gula": None,
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Delta",
        "nik": 2017, "nama": "Rahmat Hidayat", "lahir": d(datetime(1957, 11, 25)),
        "jk": "Male", "telp": "081200017", "reg": d(ago(390)),
        "visit": d(ago(70)), "sys": 148, "dia": 93, "jadwal": d(ago(18)),
        "call": d(ago(6)), "hasil": "Call again later", "hapus": None,
        "gula": 8.2, "jenis_gula": "HBA1C",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Delta",
        "nik": 2018, "nama": "Siti Nurjanah", "lahir": d(datetime(1969, 4, 19)),
        "jk": "Female", "telp": "081200018", "reg": d(ago(360)),
        "visit": d(ago(100)), "sys": 172, "dia": 110, "jadwal": d(ago(40)),
        "call": d(ago(15)), "hasil": "Refused to return", "hapus": "Refused to return",
        "gula": 220, "jenis_gula": "RBS",
    },

    # === GROUP 4: OVERDUE patients who RETURNED TO CARE (called + then visited) ===
    # These populate: "overdue returned to care" graph
    # We'll add call results in a prior month, then a recent visit
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2019, "nama": "Taufik Rahman", "lahir": d(datetime(1974, 8, 5)),
        "jk": "Male", "telp": "081200019", "reg": d(ago(420)),
        "visit": d(ago(50)), "sys": 138, "dia": 88, "jadwal": d(ago(55)),
        "call": d(ago(55)), "hasil": "Agreed to visit", "hapus": None,
        "gula": 105, "jenis_gula": "FBS",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Beta",
        "nik": 2020, "nama": "Umar Faruk", "lahir": d(datetime(1961, 1, 30)),
        "jk": "Male", "telp": "081200020", "reg": d(ago(400)),
        "visit": d(ago(48)), "sys": 132, "dia": 84, "jadwal": d(ago(52)),
        "call": d(ago(52)), "hasil": "Agreed to visit", "hapus": None,
        "gula": None, "jenis_gula": None,
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2021, "nama": "Vina Melati", "lahir": d(datetime(1976, 6, 14)),
        "jk": "Female", "telp": "081200021", "reg": d(ago(380)),
        "visit": d(ago(46)), "sys": 126, "dia": 80, "jadwal": d(ago(50)),
        "call": d(ago(50)), "hasil": "Agreed to visit", "hapus": None,
        "gula": 98, "jenis_gula": "FBS",
    },

    # === GROUP 5: NO VISIT in past 3 months (registered > 3 months ago, last visit > 90 days) ===
    # These populate: "No visit in past 3 months" graph
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2022, "nama": "Wahyu Nugroho", "lahir": d(datetime(1953, 2, 28)),
        "jk": "Male", "telp": "081200022", "reg": d(ago(450)),
        "visit": d(ago(120)), "sys": 140, "dia": 90, "jadwal": d(ago(60)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 170, "jenis_gula": "RBS",
    },
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Beta",
        "nik": 2023, "nama": "Xenia Putri", "lahir": d(datetime(1967, 12, 10)),
        "jk": "Female", "telp": "081200023", "reg": d(ago(430)),
        "visit": d(ago(110)), "sys": 155, "dia": 97, "jadwal": d(ago(50)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 7.1, "jenis_gula": "HBA1C",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2024, "nama": "Yanto Setiawan", "lahir": d(datetime(1956, 8, 4)),
        "jk": "Male", "telp": "081200024", "reg": d(ago(440)),
        "visit": d(ago(130)), "sys": 168, "dia": 104, "jadwal": d(ago(70)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 200, "jenis_gula": "RBS",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Delta",
        "nik": 2025, "nama": "Zahra Amalia", "lahir": d(datetime(1979, 4, 17)),
        "jk": "Female", "telp": "081200025", "reg": d(ago(410)),
        "visit": d(ago(140)), "sys": 146, "dia": 93, "jadwal": d(ago(80)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 9.1, "jenis_gula": "HBA1C",
    },

    # === GROUP 6: NEWLY REGISTERED (recent registration, populates "new patients" trend) ===
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Alpha",
        "nik": 2026, "nama": "Agus Suryadi", "lahir": d(datetime(1985, 10, 22)),
        "jk": "Male", "telp": "081200026", "reg": d(ago(60)),
        "visit": d(ago(5)), "sys": 136, "dia": 86, "jadwal": d(ago(-30)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 100, "jenis_gula": "FBS",
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Delta",
        "nik": 2027, "nama": "Bella Oktavia", "lahir": d(datetime(1988, 3, 8)),
        "jk": "Female", "telp": "081200027", "reg": d(ago(45)),
        "visit": d(ago(8)), "sys": 122, "dia": 76, "jadwal": d(ago(-25)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 88, "jenis_gula": "FBS",
    },

    # === GROUP 7: LOST TO FOLLOW-UP (no visit in 12+ months) ===
    # These populate: "12 month lost to follow-up" graph
    {
        "puskesmas": "PKM Sehat", "district": "Kota Utara", "shc": "SHC Beta",
        "nik": 2028, "nama": "Cahyo Wibowo", "lahir": d(datetime(1950, 5, 15)),
        "jk": "Male", "telp": "081200028", "reg": d(ago(500)),
        "visit": d(ago(380)), "sys": 158, "dia": 96, "jadwal": None,
        "call": None, "hasil": None, "hapus": None,
        "gula": None, "jenis_gula": None,
    },
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Gamma",
        "nik": 2029, "nama": "Dewi Lestari", "lahir": d(datetime(1952, 9, 20)),
        "jk": "Female", "telp": "081200029", "reg": d(ago(480)),
        "visit": d(ago(400)), "sys": 162, "dia": 100, "jadwal": None,
        "call": None, "hasil": None, "hapus": None,
        "gula": 185, "jenis_gula": "RBS",
    },

    # === PATIENT 30: DEAD patient (populates cumulative count but excluded from active) ===
    {
        "puskesmas": "PKM Makmur", "district": "Kota Selatan", "shc": "SHC Delta",
        "nik": 2030, "nama": "Eman Sulaiman", "lahir": d(datetime(1948, 7, 3)),
        "jk": "Male", "telp": "081200030", "reg": d(ago(460)),
        "visit": d(ago(200)), "sys": 180, "dia": 115, "jadwal": None,
        "call": d(ago(180)), "hasil": "Died", "hapus": "Died",
        "gula": 250, "jenis_gula": "RBS",
    },
]

for r in records:
    row = [
        r["puskesmas"],
        r["district"],
        r["shc"],
        r["nik"],
        "",  # no_rm_lama
        r["nama"],
        r["lahir"],
        r["jk"],
        r["telp"],
        r["reg"],
        r["visit"],
        r["sys"],
        r["dia"],
        r["jadwal"],
        r["call"],
        r["hasil"],
        r["hapus"],
        r["gula"],
        r["jenis_gula"],
        "Demo Region",  # wilayah
    ]
    ws.append(row)

output_path = os.path.join(os.path.dirname(__file__), "heart360_sample_30.xlsx")
wb.save(output_path)
print(f"Created: {output_path}")
print(f"Total records: {len(records)}")
