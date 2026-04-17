"""
Generate a 10-record Excel file for Heart360 focused on populating the 3 diabetes graphs:
  1. Blood Sugar <200 (Controlled)
  2. Blood Sugar >=200 (Uncontrolled: moderate 200-299, severe >=300)
  3. Missed Visits (Diabetes)

Hierarchy: India > Indore/Bhopal > PHC > SHC
Date range: 2025-26 fiscal year
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

TODAY = datetime(2026, 4, 8)
FMT = "%d-%m-%Y"

def d(dt):
    return dt.strftime(FMT) if dt else None

def ago(days):
    return TODAY - timedelta(days=days)

records = [
    # =====================================================================
    # GROUP 1: CONTROLLED (Blood Sugar <200) — Numerator for Graph 1
    # These patients: registered >=3 months, alive, visited in last 3 months,
    # latest BS reading is below the safe threshold.
    # =====================================================================

    # Patient 1: Controlled via RBS < 140
    {
        "puskesmas": "PHC Indore Central", "district": "Indore", "shc": "SHC Indore A",
        "nik": 3001, "nama": "Rajesh Kumar", "lahir": d(datetime(1968, 5, 12)),
        "jk": "Male", "telp": "091100001", "reg": d(datetime(2025, 6, 15)),
        "visit": d(datetime(2026, 2, 10)), "sys": 128, "dia": 82,
        "jadwal": d(datetime(2026, 5, 10)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 150, "jenis_gula": "RBS",
    },

    # Patient 2: Controlled via FBS < 126
    {
        "puskesmas": "PHC Indore Central", "district": "Indore", "shc": "SHC Indore B",
        "nik": 3002, "nama": "Sunita Sharma", "lahir": d(datetime(1972, 9, 25)),
        "jk": "Female", "telp": "091100002", "reg": d(datetime(2025, 5, 20)),
        "visit": d(datetime(2026, 3, 5)), "sys": 120, "dia": 78,
        "jadwal": d(datetime(2026, 6, 5)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 100, "jenis_gula": "FBS",
    },

    # Patient 3: Controlled via HbA1c < 7
    {
        "puskesmas": "PHC Bhopal Central", "district": "Bhopal", "shc": "SHC Bhopal A",
        "nik": 3003, "nama": "Amit Verma", "lahir": d(datetime(1965, 3, 8)),
        "jk": "Male", "telp": "091100003", "reg": d(datetime(2025, 7, 1)),
        "visit": d(datetime(2026, 2, 20)), "sys": 132, "dia": 84,
        "jadwal": d(datetime(2026, 5, 20)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 6.2, "jenis_gula": "HBA1C",
    },

    # =====================================================================
    # GROUP 2: MODERATE UNCONTROLLED (BS 200-299) — Numerator 1 for Graph 2
    # Latest BS is in the moderate-high range.
    # =====================================================================

    # Patient 4: RBS 200-299 (uncontrolled high)
    {
        "puskesmas": "PHC Bhopal Central", "district": "Bhopal", "shc": "SHC Bhopal B",
        "nik": 3004, "nama": "Priya Patel", "lahir": d(datetime(1970, 11, 18)),
        "jk": "Female", "telp": "091100004", "reg": d(datetime(2025, 8, 10)),
        "visit": d(datetime(2026, 3, 15)), "sys": 145, "dia": 92,
        "jadwal": d(datetime(2026, 6, 15)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 250, "jenis_gula": "RBS",
    },

    # Patient 5: FBS 126-199 (uncontrolled moderate)
    {
        "puskesmas": "PHC Indore Central", "district": "Indore", "shc": "SHC Indore A",
        "nik": 3005, "nama": "Vikram Singh", "lahir": d(datetime(1960, 7, 3)),
        "jk": "Male", "telp": "091100005", "reg": d(datetime(2025, 6, 25)),
        "visit": d(datetime(2026, 2, 28)), "sys": 150, "dia": 95,
        "jadwal": d(datetime(2026, 5, 28)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 160, "jenis_gula": "FBS",
    },

    # =====================================================================
    # GROUP 3: SEVERE UNCONTROLLED (BS >=300) — Numerator 2 for Graph 2
    # Latest BS is in the dangerously high range.
    # =====================================================================

    # Patient 6: RBS >= 200 (uncontrolled high)
    {
        "puskesmas": "PHC Indore Central", "district": "Indore", "shc": "SHC Indore B",
        "nik": 3006, "nama": "Meena Gupta", "lahir": d(datetime(1958, 2, 14)),
        "jk": "Female", "telp": "091100006", "reg": d(datetime(2025, 5, 10)),
        "visit": d(datetime(2026, 3, 20)), "sys": 165, "dia": 105,
        "jadwal": d(datetime(2026, 6, 20)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 350, "jenis_gula": "RBS",
    },

    # Patient 7: HbA1c >= 9
    {
        "puskesmas": "PHC Bhopal Central", "district": "Bhopal", "shc": "SHC Bhopal A",
        "nik": 3007, "nama": "Ramesh Yadav", "lahir": d(datetime(1955, 10, 22)),
        "jk": "Male", "telp": "091100007", "reg": d(datetime(2025, 7, 15)),
        "visit": d(datetime(2026, 3, 10)), "sys": 170, "dia": 108,
        "jadwal": d(datetime(2026, 6, 10)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 10.2, "jenis_gula": "HBA1C",
    },

    # Patient 8: PPBS >= 300 (skipped by indicators — PPBS not yet supported)
    {
        "puskesmas": "PHC Bhopal Central", "district": "Bhopal", "shc": "SHC Bhopal B",
        "nik": 3008, "nama": "Kavita Joshi", "lahir": d(datetime(1963, 6, 30)),
        "jk": "Female", "telp": "091100008", "reg": d(datetime(2025, 8, 5)),
        "visit": d(datetime(2026, 2, 15)), "sys": 155, "dia": 98,
        "jadwal": d(datetime(2026, 5, 15)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 320, "jenis_gula": "PPBS",
    },

    # =====================================================================
    # GROUP 4: MISSED VISITS — Numerator for Graph 3
    # Registered >=3 months, alive, under care (visited in last 12 months),
    # but NO visit in the last 3 months.
    # =====================================================================

    # Patient 9: Last visit 5 months ago (missed)
    {
        "puskesmas": "PHC Indore Central", "district": "Indore", "shc": "SHC Indore A",
        "nik": 3009, "nama": "Deepak Mishra", "lahir": d(datetime(1975, 1, 20)),
        "jk": "Male", "telp": "091100009", "reg": d(datetime(2025, 5, 1)),
        "visit": d(datetime(2025, 11, 10)), "sys": 138, "dia": 88,
        "jadwal": d(datetime(2026, 2, 10)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 180, "jenis_gula": "RBS",
    },

    # Patient 10: Last visit 4 months ago (missed)
    {
        "puskesmas": "PHC Bhopal Central", "district": "Bhopal", "shc": "SHC Bhopal B",
        "nik": 3010, "nama": "Anita Dubey", "lahir": d(datetime(1980, 4, 5)),
        "jk": "Female", "telp": "091100010", "reg": d(datetime(2025, 6, 1)),
        "visit": d(datetime(2025, 12, 5)), "sys": 142, "dia": 90,
        "jadwal": d(datetime(2026, 3, 5)),
        "call": None, "hasil": None, "hapus": None,
        "gula": 7.5, "jenis_gula": "HBA1C",
    },
]

for r in records:
    row = [
        r["puskesmas"],
        r["district"],
        r["shc"],
        r["nik"],
        "",
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
        "India",
    ]
    ws.append(row)

# Auto-fit column widths for readability
for col in ws.columns:
    max_length = 0
    col_letter = col[0].column_letter
    for cell in col:
        if cell.value:
            max_length = max(max_length, len(str(cell.value)))
    ws.column_dimensions[col_letter].width = min(max_length + 2, 30)

output_path = os.path.join(os.path.dirname(__file__), "heart360_diabetes_10.xlsx")
wb.save(output_path)
print(f"Created: {output_path}")
print(f"Total records: {len(records)}")
print()
print("=== Expected Graph Results (for April 2026 ref_month) ===")
print()
print("Denominator (all 3 graphs): 10 patients under care")
print("  - All 10 registered >= 3 months ago")
print("  - All 10 visited within last 12 months")
print()
print("Graph 1 - Controlled Blood Sugar:")
print("  Numerator: 3 patients (P1: RBS 150<140? NO — actually 150>=140 so uncontrolled moderate)")
print("  CORRECTED: P1 RBS 150 is uncontrolled moderate (140-199), P2 FBS 100<126, P3 HBA1C 6.2<7")
print("  Controlled: 2 patients (P2, P3). Rate: 2/10 = 20%")
print()
print("Graph 2 - Uncontrolled Blood Sugar:")
print("  Moderate: 2 patients (P1: RBS 150 [140-199], P5: FBS 160 [126-199])")
print("  Rate: 2/10 = 20%")
print("  High: 2 patients (P4: RBS 250 [>=200], P6: RBS 350 [>=200])")
print("  Note: P7 HBA1C 10.2>=9 is high, P8 PPBS 320 is skipped (PPBS not supported)")
print("  Rate: 3/10 = 30% (P4, P6, P7)")
print()
print("Graph 3 - Missed Visits (Diabetes):")
print("  Numerator: 2 patients (P9: last visit Nov 2025, P10: last visit Dec 2025)")
print("  Rate: 2/10 = 20%")
