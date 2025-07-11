import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3

def log_write(f, message):
    timestamp = datetime.now().strftime("%Y-%B-%d-%H-%M-%S")
    f.write(f"[{timestamp}]    {message}\n")

def extract_data(save_path):
    url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    response = requests.get(url)
    
    if response.status_code != 200:
        raise RuntimeError(f"Falied to fetch webpage. Status Code: {response.status_code}")
    
    soup = BeautifulSoup(response.text, 'html.parser')
        
    # GDP 정보를 포함하는 table tag 찾기
    table = soup.find("table", class_="wikitable")
    if table is None:
        raise RuntimeError("Table not found.")
    
    # 해당 table에서 실제 데이터를 담고 있는 tbody tag 찾기
    tbody = table.find("tbody")
    if tbody is None:
        raise RuntimeError("tbody is not found.")
    
    # 테이블의 각 row 정보를 담고 있는 tr tag 찾기
    trs = tbody.find_all("tr")
    if not trs or len(trs) < 2:
        raise RuntimeError("No rows found in table.")

    # 테이블의 header에서 IMF에 해당하는 위치 정보만 추출
    header = trs[0].find_all("th")

    imf_idx = None
    imf_colspan = 1
    imf_cols = ['Country']

    for idx, th in enumerate(header[1:]):
        if "IMF" in th.get_text():
            imf_idx = idx
            imf_colspan = int(th.get("colspan", 1))
            break
    
    if imf_idx is None or imf_idx < 0:
        raise ValueError("IMF header is not found.")

    # 테이블의 subheader에서 컬럼명을 추출
    subheader = trs[1].find_all("th")
    if not subheader:
        raise RuntimeError("Subheader is not found.")

    imf_cols.extend([subheader[imf_idx + i].get_text() for i in range(imf_colspan)])

    # 4. tr을 반복적으로 순회하며 값 가져오기
    rows = []
    for tr in trs[2:]:
        tds = tr.find_all("td")
        if len(tds) < imf_idx + imf_colspan:
            continue
        
        row = {}
        for i in range(imf_colspan + 1):
            colname = imf_cols[i]
            value = tds[imf_idx + i].get_text(strip=True)
            row[colname] = value

        rows.append(row)

    if not rows:
        raise RuntimeError("No data rows were extracted.")
        
    df = pd.DataFrame(rows)
    df.to_json(save_path + "Countries_by_GDP.json", orient="records", indent=4, force_ascii = False)
    return
        
def transform_data(save_path):
    df = pd.read_json(save_path + "Countries_by_GDP.json")

    # world 행 제거
    df = df[df["Country"] != "World"]

    # 불필요한 문자 제거 및 결측값 처리
    df = df.replace(r'[-—]', None, regex=True)
    df = df.replace(r'\[.*?\]', '', regex=True)
    df = df.replace(r',', '', regex=True)

    # Forecast 열의 형 변환 및 단위 변경 (1B USD)
    df['Forecast'] = df['Forecast'].astype(float)
    df['Forecast'] = (df['Forecast'] / 1000).round(2)

    # Year 열의 형 변환
    df['Year'] = df['Year'].astype('Int64')

    # GDP가 높은 순서대로 정렬
    df = df.sort_values(by='Forecast', ascending=False)

    # 컬럼명 수정
    df = df.rename(columns = {'Forecast' : 'GDP_USD_billion'})

    df.to_json(save_path + "Countries_by_GDP.json", orient="records", indent=4, force_ascii = False)
    return

# 데이터베이스에 저장
def load_data(save_path):
    df = pd.read_json(save_path + "Countries_by_GDP.json")

    conn = sqlite3.connect(save_path + "World_Economies.db")

    df.to_sql("Countries_by_GDP", conn, if_exists="replace", index = False)
    conn.close()
    return


def get_region(url):
    response = requests.get(url)

    if response.status_code != 200:
        raise RuntimeError(f"Falied to fetch webpage. Status Code: {response.status_code}")
    
    soup = BeautifulSoup(response.text, 'html.parser')

    # Region 정보를 포함하는 table tag 찾기
    table = soup.find("table", class_="wikitable")
    if table is None:
        raise RuntimeError("Table not found.")
    
    # 테이블의 각 row 정보를 담고 있는 tr tag 찾기
    trs = table.find_all("tr")
    if not trs or len(trs) < 2:
        raise RuntimeError("No rows found in table.")
    
    header_idx = None
    ths = trs[0].find_all("th")
    for idx, th in enumerate(ths):
        if 'Country' in th.get_text():
            header_idx = idx
            break

    if header_idx == None :
        raise RuntimeError("No header found in table.")

    country_list = []
    for tr in trs[1:]:
        tds = tr.find_all("td")
        if len(tds) < 2 or tds[0].get_text().strip() == "—":
            continue
        country = tds[header_idx].get_text(strip = True)
        country_list.append(country)

    return country_list

def extract_region(save_path):
    url_dict = {'Africa' : 'https://en.wikipedia.org/wiki/List_of_African_countries_by_GDP_(nominal)',
                'Arab' : 'https://en.wikipedia.org/wiki/List_of_Arab_League_countries_by_GDP_(nominal)',
                'Asia' : 'https://en.wikipedia.org/wiki/List_of_countries_in_Asia-Pacific_by_GDP_(nominal)',
                'Latin' : 'https://en.wikipedia.org/wiki/List_of_Latin_American_and_Caribbean_countries_by_GDP_(nominal)',
                'North America' : 'https://en.wikipedia.org/wiki/List_of_North_American_countries_by_GDP_(nominal)',
                'Oceania' : 'https://en.wikipedia.org/wiki/List_of_Oceanian_countries_by_GDP',
                'Europe' : 'https://en.wikipedia.org/wiki/List_of_sovereign_states_in_Europe_by_GDP_(nominal)'
                }
    
    # country 열, region 열로 구성된 row 구성
    rows = []
    for region, url in url_dict.items():
        country_list = get_region(url)

        for country in country_list:
            rows.append({'Country' : country, 'Region': region})

    df = pd.DataFrame(rows)
    df.to_json(save_path + "Countries_with_Region.json", orient="records", indent=4, force_ascii = False)
    return

def transform_region(save_path):
    df = pd.read_json(save_path + "Countries_with_Region.json")

    # 불필요한 문자 제거 및 결측값 처리
    df = df.replace(r'[-—]', None, regex=True)
    df = df.replace(r'\[.*?\]', '', regex=True)
    df = df.replace(r',', '', regex=True)

    df.to_json(save_path + "Countries_with_Region.json", orient="records", indent=4, force_ascii = False)
    return

def load_region(save_path):
    df = pd.read_json(save_path + "Countries_with_Region.json")

    conn = sqlite3.connect(save_path + "World_Economies.db")

    df.to_sql("Countries_with_Region", conn, if_exists="replace", index = False)
    conn.close()
    return

def filter_data(path):
    conn = sqlite3.connect(path + 'World_Economies.db')
    cur = conn.cursor()

    # GDP가 100B USD 이상이 되는 국가
    cur.execute(
        """
        SELECT Country, GDP_USD_billion
        FROM Countries_by_GDP
        WHERE GDP_USD_billion > 100 
        ORDER BY GDP_USD_billion DESC
        """
    )

    rows = cur.fetchall()
    gdp_df = pd.DataFrame(rows, columns = ['Country', 'GDP_USD_billion'])

    # 각 Region별로 top5 국가의 평균을 구해서 화면에 출력
    # 두 데이터를 Country 기준으로 merge
    cur.execute(
        """
        WITH Ranked AS (
            SELECT G.Country, G.GDP_USD_billion, R.Region,
            ROW_NUMBER() OVER (
                PARTITION BY R.Region ORDER BY G.GDP_USD_billion DESC
            ) AS RN
            FROM Countries_by_GDP AS G
            LEFT JOIN Countries_with_Region as R
            ON G.Country = R.Country
        )

        SELECT REGION, AVG(GDP_USD_billion) AS Top_5_mean_GDP
        FROM Ranked
        WHERE RN <= 5 AND Region IS NOT NULL
        GROUP BY Region
        ORDER BY Top_5_mean_GDP DESC
        """
    )

    rows = cur.fetchall()
    top_5_df = pd.DataFrame(rows, columns = ['Country', 'Top_5_mean_GDP'])

    conn.close()
    return gdp_df, top_5_df

def main():
    path = "/Users/admin/Desktop/Softeer_DE_6th/missions/W1/"
    
    with open(path + "etl_project_log.txt", "a") as log_file:
        log_write(log_file, "ETL Process Started.")

        try:
            # Extract
            log_write(log_file, "Start extracting data.")
            extract_data(path)
            log_write(log_file, "Finish extracting data.")
          
            # Transform
            log_write(log_file, "Start transforming data.")
            transform_data(path)
            log_write(log_file, "Finish transforming data.")
            
            # Load
            log_write(log_file, "Start loading data.")
            load_data(path)
            log_write(log_file, "Finish loading data.")
            
        except Exception as e:
            log_write(log_file, f"Error during extracting data: {e}")
            raise

        log_write(log_file, "ETL Process Finished.")

    try:
        extract_region(path)
        transform_region(path)
        load_region(path)

        gdp_df, top_5_df = filter_data(path)

        print("GDP가 100B USD 이상이 되는 국가:")
        print(gdp_df)

        print("Region별로 top5 국가의 평균: ")
        print(top_5_df)

    except Exception as e:
        raise

if __name__ == "__main__":
    main()