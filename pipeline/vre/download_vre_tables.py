import argparse
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.request import Request, urlopen

tables = [
  'B15002',  # Sex by Educational Attainment
  'C24010',  # Sex by Occupation for the Civilian Population
  'B03002',  # Hispanic or Latino Origin by Race
]

sumlvls_all = [
  '010',  # Nation
  '040',  # State
  '050',  # County
  '060',  # County Subdivision
  '160',  # Place
  '310',  # MSA
  '500',  # Congressional District
  '860',  # Zip Code
]

sumlvls_state = [
  '140',  # Tract
  '150',  # Block Group
]

# fmt: off
statecodes = [
  '01', '02', '04', '05', '06', '08',
  '09', '10', '11', '12', '13', '15',
  '16', '17', '18', '19', '20', '21',
  '22', '23', '24', '25', '26', '27',
  '28', '29', '30', '31', '32', '33',
  '34', '35', '36', '37', '38', '39',
  '40', '41', '42', '44', '45', '46',
  '47', '48', '49', '50', '51', '53',
  '54', '55', '56', '72',
]
# fmt: on


def download_and_extract(year: int, table: str, sumlvl: str, statecode: str = '') -> None:
  statecode_inline = ''
  if statecode != '':
    statecode_inline = f'_{statecode}'

  url = (
    f'https://www2.census.gov/programs-surveys/acs/replicate_estimates/'
    f'{year}/data/5-year/{sumlvl}/{table}{statecode_inline}.csv.zip'
  )
  out_dir = Path(table)
  out_dir.mkdir(parents=True, exist_ok=True)
  out_path = out_dir / f'{sumlvl}{statecode_inline}.csv'

  print(f'Downloading {url} -> {out_path}')
  req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
  with urlopen(req) as resp:
    data = resp.read()

  with zipfile.ZipFile(BytesIO(data)) as zf:
    csv_members = [name for name in zf.namelist() if name.lower().endswith('.csv')]
    if len(csv_members) != 1:
      raise ValueError(f'Expected exactly one CSV in ZIP, found {csv_members}')

    with zf.open(csv_members[0]) as src, out_path.open('wb') as dst:
      dst.write(src.read())


def concatenate_csv_in_dir(table: str, sumlvl: str) -> None:
  table_dir = Path(table)
  out_path = table_dir / f'{sumlvl}.csv'
  parts = sorted(
    p for p in table_dir.glob(f'{sumlvl}_*.csv')
    if p.is_file()
  )

  if not parts:
    return

  print(f'Concatenating {len(parts)} files -> {out_path}')
  with out_path.open('wb') as out_f:
    for idx, part in enumerate(parts):
      with part.open('rb') as in_f:
        lines = in_f.readlines()

      if idx == 0:
        out_f.writelines(lines)
      else:
        # All state-split files repeat header + 2 description rows.
        out_f.writelines(lines[3:])

  for part in parts:
    part.unlink()


if __name__ == '__main__':
  ap = argparse.ArgumentParser()
  ap.add_argument('--year', type=int, default=2024)
  args = ap.parse_args()

  for i in tables:
    for j in sumlvls_all:
      try:
        download_and_extract(args.year, i, j)
      except Exception as e:
        print(f'Failed to download or extract {i}/{j}: {e}')

    for j in sumlvls_state:
      for k in statecodes:
        try:
          download_and_extract(args.year, i, j, k)
        except Exception as e:
          print(f'Failed to download or extract {i}/{j}_{k}: {e}')
      try:
        concatenate_csv_in_dir(i, j)
      except Exception as e:
        print(f'Failed to concatenate {i}/{j}: {e}')
